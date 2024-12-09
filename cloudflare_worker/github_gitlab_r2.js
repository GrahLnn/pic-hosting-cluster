// 用户配置区域开始 =================================

// GitLab 节点配置
const GITLAB_CONFIGS = [
  { name: '', id: '', token: '' },  // GitLab 账户1
  { name: '', id: '', token: '' },  // GitLab 账户2
  { name: '', id: '', token: '' },  // GitLab 账户3
  { name: '', id: '', token: '' },  // GitLab 账户4
];

// GitHub 配置
const GITHUB_REPOS = [''];  // GitHub 仓库名列表
const GITHUB_USERNAME = '';  // GitHub 用户名
const GITHUB_PAT = '';  // GitHub 个人访问令牌

// R2 存储配置
const R2_CONFIGS = [
  {
    name: '',  // 帐户1 ID
    accountId: '',  // 帐户1 访问密钥 ID
    accessKeyId: '',  // 帐户1 机密访问密钥
    secretAccessKey: '',  // 帐户1 机密访问密钥
    bucket: '' // 帐户1 R2 存储桶名称
  },
  {
    name: '',  // 帐户2 ID
    accountId: '',  // 帐户2 访问密钥 ID
    accessKeyId: '',  // 帐户2 机密访问密钥
    secretAccessKey: '',  // 帐户2 机密访问密钥
    bucket: '' // 帐户2 R2 存储桶名称
  },
  // 可以添加更多 R2 配置
];

// 定义集群访问目录
const DIR = '';

// 定义集群里全部节点连接状态的密码验证，区分大小写（优先使用自定义密码，若为空则使用 GITHUB_PAT）
const CHECK_PASSWORD = '' || GITHUB_PAT;


// 用户配置区域结束 =================================

// AWS SDK 签名相关函数开始 =================================

// 获取签名URL
async function getSignedUrl(r2Config, method, path) {
  const region = 'auto';
  const service = 's3';
  const host = `${r2Config.accountId}.r2.cloudflarestorage.com`;
  const datetime = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
  const date = datetime.substr(0, 8);

  const canonicalRequest = [
    method,
    '/' + path,
    '',
    `host:${host}`,
    'x-amz-content-sha256:UNSIGNED-PAYLOAD',
    `x-amz-date:${datetime}`,
    '',
    'host;x-amz-content-sha256;x-amz-date',
    'UNSIGNED-PAYLOAD'
  ].join('\n');

  const stringToSign = [
    'AWS4-HMAC-SHA256',
    datetime,
    `${date}/${region}/${service}/aws4_request`,
    await sha256(canonicalRequest)
  ].join('\n');

  const signature = await getSignature(
    r2Config.secretAccessKey,
    date,
    region,
    service,
    stringToSign
  );

  const authorization = [
    `AWS4-HMAC-SHA256 Credential=${r2Config.accessKeyId}/${date}/${region}/${service}/aws4_request`,
    `SignedHeaders=host;x-amz-content-sha256;x-amz-date`,
    `Signature=${signature}`
  ].join(', ');

  return {
    url: `https://${host}/${path}`,
    headers: {
      'Authorization': authorization,
      'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
      'x-amz-date': datetime,
      'Host': host
    }
  };
}

// SHA256 哈希函数
async function sha256(message) {
  const msgBuffer = new TextEncoder().encode(message);
  const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
  return Array.from(new Uint8Array(hashBuffer))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

// HMAC-SHA256 函数
async function hmacSha256(key, message) {
  const keyBuffer = key instanceof ArrayBuffer ? key : new TextEncoder().encode(key);
  const messageBuffer = new TextEncoder().encode(message);

  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyBuffer,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );

  const signature = await crypto.subtle.sign(
    'HMAC',
    cryptoKey,
    messageBuffer
  );

  return signature;
}

// 获取签名
async function getSignature(secret, date, region, service, stringToSign) {
  const kDate = await hmacSha256('AWS4' + secret, date);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, service);
  const kSigning = await hmacSha256(kService, 'aws4_request');
  const signature = await hmacSha256(kSigning, stringToSign);

  return Array.from(new Uint8Array(signature))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
}

export default {
  async fetch(request, env, ctx) {
    // 检查缓存
    const cacheUrl = new URL(request.url);
    const cacheKey = new Request(cacheUrl.toString(), request);
    const cache = caches.default;
    let cacheResponse = await cache.match(cacheKey);

    if (cacheResponse) {
      return cacheResponse;
    }

    const isValidGithubRepos = Array.isArray(GITHUB_REPOS) &&
      GITHUB_REPOS.length > 0 &&
      GITHUB_REPOS.some(repo => repo.trim() !== '');

    const githubRepos = isValidGithubRepos
      ? GITHUB_REPOS.filter(repo => repo.trim() !== '')
      : GITLAB_CONFIGS.map(config => config.name);

    const url = new URL(request.url);
    const FILE = url.pathname.split('/').pop();
    const from = url.searchParams.get('from')?.toLowerCase();

    if (url.pathname === `/${CHECK_PASSWORD}`) {
      const response = await listProjects(GITLAB_CONFIGS, githubRepos, GITHUB_USERNAME, GITHUB_PAT);
      // 不缓存状态检查页面
      return response;
    }

  const startTime = Date.now();

  // 根据不同的访问方式构建请求
  let requests = [];

  // R2 请求生成函数
  const generateR2Requests = async () => {
    return Promise.all(R2_CONFIGS.map(async (r2Config) => {
      const r2Path = `${r2Config.bucket}/${DIR}/${FILE}`;
      const signedRequest = await getSignedUrl(r2Config, 'GET', r2Path);
      return {
        url: signedRequest.url,
        headers: signedRequest.headers,
        source: 'r2',
        repo: `${r2Config.name} (${r2Config.bucket})`
      };
    }));
  };

  if (from === 'where') {
    // 获取文件信息模式
    const githubRequests = githubRepos.map(repo => ({
      url: `https://api.github.com/repos/${GITHUB_USERNAME}/${repo}/contents/${DIR}/${FILE}`,
      headers: {
        'Authorization': `token ${GITHUB_PAT}`,
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Cloudflare Worker'
      },
      source: 'github',
      repo: repo,
      processResponse: async (response) => {
        if (!response.ok) throw new Error('Not found');
        const data = await response.json();
        return {
          size: data.size,
          exists: true
        };
      }
    }));

    const gitlabRequests = GITLAB_CONFIGS.map(config => ({
      url: `https://gitlab.com/api/v4/projects/${config.id}/repository/files/${encodeURIComponent(`${DIR}/${FILE}`)}?ref=main`,
      headers: {
        'PRIVATE-TOKEN': config.token
      },
      source: 'gitlab',
      repo: config.name,
      processResponse: async (response) => {
        if (!response.ok) throw new Error('Not found');
        const data = await response.json();
        const size = atob(data.content).length;
        return {
          size: size,
          exists: true
        };
      }
    }));

    const r2Requests = await generateR2Requests();
    const r2WhereRequests = r2Requests.map(request => ({
      ...request,
      processResponse: async (response) => {
        if (!response.ok) throw new Error('Not found');
        const size = response.headers.get('content-length');
        return {
          size: parseInt(size),
          exists: true
        };
      }
    }));

    requests = [...githubRequests, ...gitlabRequests, ...r2WhereRequests];

  } else {
    // 获取文件内容模式
    if (from === 'github') {
      requests = githubRepos.map(repo => ({
        url: `https://raw.githubusercontent.com/${GITHUB_USERNAME}/${repo}/main/${DIR}/${FILE}`,
        headers: {
          'Authorization': `token ${GITHUB_PAT}`,
          'User-Agent': 'Cloudflare Worker'
        },
        source: 'github',
        repo: repo
      }));
    } else if (from === 'gitlab') {
      requests = GITLAB_CONFIGS.map(config => ({
        url: `https://gitlab.com/api/v4/projects/${config.id}/repository/files/${encodeURIComponent(`${DIR}/${FILE}`)}/raw?ref=main`,
        headers: {
          'PRIVATE-TOKEN': config.token
        },
        source: 'gitlab',
        repo: config.name
      }));
    } else if (from === 'r2') {
      requests = await generateR2Requests();
    } else {
      // 如果没有指定来源，则从所有源获取
      const githubRequests = githubRepos.map(repo => ({
        url: `https://raw.githubusercontent.com/${GITHUB_USERNAME}/${repo}/main/${DIR}/${FILE}`,
        headers: {
          'Authorization': `token ${GITHUB_PAT}`,
          'User-Agent': 'Cloudflare Worker'
        },
        source: 'github',
        repo: repo
      }));

      const gitlabRequests = GITLAB_CONFIGS.map(config => ({
        url: `https://gitlab.com/api/v4/projects/${config.id}/repository/files/${encodeURIComponent(`${DIR}/${FILE}`)}/raw?ref=main`,
        headers: {
          'PRIVATE-TOKEN': config.token
        },
        source: 'gitlab',
        repo: config.name
      }));

      const r2Requests = await generateR2Requests();

      requests = [...githubRequests, ...gitlabRequests, ...r2Requests];
    }
  }

  // 发送请求并处理响应
  const fetchPromises = requests.map(request => {
    const { url, headers, source, repo, processResponse } = request;

    return fetch(new Request(url, {
      method: 'GET',
      headers: headers
    })).then(async response => {
      if (from === 'where' && typeof processResponse === 'function') {
        // 使用 `processResponse` 处理 where 查询逻辑
        try {
          const result = await processResponse(response);
          const endTime = Date.now();
          const duration = endTime - startTime;

          const formattedSize = result.size > 1024 * 1024
            ? `${(result.size / (1024 * 1024)).toFixed(2)} MB`
            : `${(result.size / 1024).toFixed(2)} kB`;

          return {
            fileName: FILE,
            size: formattedSize,
            source: `${source} (${repo})`,
            duration: `${duration}ms`
          };
        } catch (error) {
          throw new Error(`Not found in ${source} (${repo})`);
        }
      } else {
        // 对于内容获取，直接返回响应
        if (!response.ok) {
          throw new Error(`Not found in ${source} (${repo})`);
        }
        return response;
      }
    }).catch(error => {
      throw new Error(`Error in ${source} (${repo}): ${error.message}`);
    });
  });

  try {
    if (requests.length === 0) {
      throw new Error('No valid source specified');
    }

    const result = await Promise.any(fetchPromises);

    let response;
    if (from === 'where') {
      response = new Response(JSON.stringify(result, null, 2), {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*'
        }
      });
    } else if (result instanceof Response) {
      response = new Response(result.body, result);
      // 清除敏感header
      response.headers.delete('Authorization');
      response.headers.delete('PRIVATE-TOKEN');
      response.headers.delete('x-amz-content-sha256');
      response.headers.delete('x-amz-date');
    } else {
      throw new Error("Unexpected result type");
    }

    // 添加缓存控制头
    response.headers.append("Cache-Control", "s-maxage=31556952");

    // 异步缓存响应
    ctx.waitUntil(cache.put(cacheKey, response.clone()));

    return response;

  } catch (error) {
    const sourceText = from === 'where'
      ? 'in any repository'
      : from
        ? `from ${from}`
        : 'in the GitHub, GitLab and R2 storage';

    const errorResponse = new Response(
      `404: Cannot find the ${FILE} ${sourceText}.`,
      { status: 404 }
    );

    // 不缓存错误响应
    return errorResponse;
  }
}
};

// 列出所有节点仓库状态
async function listProjects(gitlabConfigs, githubRepos, githubUsername, githubPat) {
  let result = 'GitHub, GitLab and R2 Storage status:\n\n';

  try {
    // 并发执行所有检查
    const [username, ...allChecks] = await Promise.all([
      getGitHubUsername(githubPat),
      ...githubRepos.map(repo =>
        checkGitHubRepo(githubUsername, repo, githubPat)
      ),
      ...gitlabConfigs.map(config =>
        checkGitLabProject(config.id, config.token)
      ),
      ...R2_CONFIGS.map(config =>
        checkR2Storage(config)
      )
    ]);

    // 计算各类检查结果的数量
    const githubCount = githubRepos.length;
    const gitlabCount = gitlabConfigs.length;

    // 分割检查结果
    const githubResults = allChecks.slice(0, githubCount);
    const gitlabResults = allChecks.slice(githubCount, githubCount + gitlabCount);
    const r2Results = allChecks.slice(githubCount + gitlabCount);

    // 添加 GitHub 结果
    githubRepos.forEach((repo, index) => {
      const [status, fileCount, totalSize] = githubResults[index];
      const formattedSize = formatSize(totalSize);
      result += `GitHub: ${repo} - ${status} (Username: ${username}, Files: ${fileCount}, Size: ${formattedSize})\n`;
    });

    // 添加 GitLab 结果
    gitlabConfigs.forEach((config, index) => {
      const [status, username, fileCount] = gitlabResults[index];
      result += `GitLab: Project ID ${config.id} - ${status} (Username: ${username}, Files: ${fileCount})\n`;
    });

    // 添加 R2 结果
    r2Results.forEach(([status, name, bucket]) => {
      result += `R2 Storage: ${name} - ${status} (Bucket: ${bucket})\n`;
    });

  } catch (error) {
    result += `Error during status check: ${error.message}\n`;
  }

  return new Response(result, {
    headers: { 'Content-Type': 'text/plain' }
  });
}

// 文件大小格式化函数
function formatSize(sizeInBytes) {
  if (sizeInBytes >= 1024 * 1024 * 1024) {
    return `${(sizeInBytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  } else if (sizeInBytes >= 1024 * 1024) {
    return `${(sizeInBytes / (1024 * 1024)).toFixed(2)} MB`;
  } else {
    return `${(sizeInBytes / 1024).toFixed(2)} kB`;
  }
}

// 获取 GitHub 用户名的异步函数
async function getGitHubUsername(pat) {
  const url = 'https://api.github.com/user';  // GitHub 用户信息 API 地址
  try {
    const response = await fetch(url, {
      headers: {
        'Authorization': `token ${pat}`,  // 使用个人访问令牌进行授权
        'Accept': 'application/vnd.github.v3+json',  // 指定接受的响应格式
        'User-Agent': 'Cloudflare Worker'  // 用户代理
      }
    });

    // 如果响应状态为 200，表示成功
    if (response.status === 200) {
      const data = await response.json();  // 解析 JSON 数据
      return data.login;  // 返回用户登录名
    } else {
      console.error('GitHub API Error:', response.status);  // 记录错误状态
      return 'Unknown';  // 返回未知状态
    }
  } catch (error) {
    console.error('GitHub request error:', error);  // 记录请求错误
    return 'Error';  // 返回错误状态
  }
}

// 检查 GitHub 仓库的异步函数
async function checkGitHubRepo(owner, repo, pat) {
  const repoUrl = `https://api.github.com/repos/${owner}/${repo}`;
  const contentsUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${DIR}`; // 直接检查指定目录

  const headers = {
    'Authorization': `token ${pat}`,
    'Accept': 'application/vnd.github.v3+json',
    'User-Agent': 'Cloudflare Worker'
  };

  try {
    // 并发请求获取仓库信息和目录内容
    const [repoResponse, contentsResponse] = await Promise.all([
      fetch(repoUrl, { headers }),
      fetch(contentsUrl, { headers })
    ]);

    const repoData = await repoResponse.json();

    if (repoResponse.status !== 200) {
      throw new Error(`Repository error: ${repoData.message}`);
    }

    if (contentsResponse.status !== 200) {
      return [`working (${repoData.private ? 'private' : 'public'})`, 0, 0];
    }

    const contentsData = await contentsResponse.json();

    // 计算文件数量和总大小
    const fileStats = contentsData.reduce((acc, item) => {
      if (item.type === 'file') {
        return {
          count: acc.count + 1,
          size: acc.size + (item.size || 0)
        };
      }
      return acc;
    }, { count: 0, size: 0 });

    return [
      `working (${repoData.private ? 'private' : 'public'})`,
      fileStats.count,
      fileStats.size
    ];

  } catch (error) {
    console.error(`Error checking GitHub repo ${repo}:`, error);
    return [`error: ${error.message}`, 0, 0];
  }
}

// 检查 GitLab 项目的异步函数
async function checkGitLabProject(projectId, pat) {
  const projectUrl = `https://gitlab.com/api/v4/projects/${projectId}`;
  const filesUrl = `https://gitlab.com/api/v4/projects/${projectId}/repository/tree?path=${DIR}&per_page=100`; // 使用 per_page 参数增加返回数量

  try {
    const [projectResponse, filesResponse] = await Promise.all([
      fetch(projectUrl, {
        headers: {
          'PRIVATE-TOKEN': pat
        }
      }),
      fetch(filesUrl, {
        headers: {
          'PRIVATE-TOKEN': pat
        }
      })
    ]);

    if (projectResponse.status === 200) {
      const projectData = await projectResponse.json();
      let fileCount = 0;

      if (filesResponse.status === 200) {
        const filesData = await filesResponse.json();

        // 只计算文件数量（不包括目录）
        fileCount = filesData.filter(file => file.type === 'blob').length;
      }

      return [
        `working (${projectData.visibility})`,
        projectData.owner.username,
        fileCount
      ];
    } else if (projectResponse.status === 404) {
      return ['not found', 'Unknown', 0];
    } else {
      return ['disconnect', 'Unknown', 0];
    }
  } catch (error) {
    return ['disconnect', 'Error', 0];
  }
}

// 检查 R2 存储状态
async function checkR2Storage(r2Config) {
  try {
    const testPath = `${r2Config.bucket}/${DIR}/test-access`;
    const signedRequest = await getSignedUrl(r2Config, 'HEAD', testPath);

    const response = await fetch(signedRequest.url, {
      method: 'HEAD',
      headers: signedRequest.headers
    });

    // 即使文件不存在，只要能访问到存储桶就认为是正常的
    const status = response.status === 404 ? 'working' : 'error';

    return [
      status,
      r2Config.name,
      r2Config.bucket
    ];
  } catch (error) {
    return ['error', r2Config.name, 'connection failed'];
  }
}

/* 由于 GitLab 的文件大小需要逐一查询，当文件量稍大时并发量过大而导致 Worker 报以下错误，所以取消。看哪位哥哥有方案。
Error: Worker exceeded CPU time limit.
Uncaught (in response) Error: Worker exceeded CPU time limit.

async function checkGitLabProject(projectId, pat) {
const projectUrl = `https://gitlab.com/api/v4/projects/${projectId}`;
const filesUrl = `https://gitlab.com/api/v4/projects/${projectId}/repository/tree?path=images&recursive=true`;

try {
  const [projectResponse, filesResponse] = await Promise.all([
    fetch(projectUrl, {
      headers: {
        'PRIVATE-TOKEN': pat
      }
    }),
    fetch(filesUrl, {
      headers: {
        'PRIVATE-TOKEN': pat
      }
    })
  ]);

  if (projectResponse.status === 200) {
    const projectData = await projectResponse.json();
    let totalSize = 0;
    let fileCount = 0;

    if (filesResponse.status === 200) {
      const filesData = await filesResponse.json();

      // 过滤路径为 images/ 开头的文件
      const imageFiles = filesData.filter(file => file.type === 'blob' && file.path.startsWith('images/'));

      // 更新文件数量
      fileCount = imageFiles.length;

      // 并发获取每个文件的大小
      const sizePromises = imageFiles.map(file => getFileSizeFromGitLab(projectId, file.path, pat));

      const sizes = await Promise.all(sizePromises);
      totalSize = sizes.reduce((acc, size) => acc + size, 0);
    }

    return [`working (${projectData.visibility})`, projectData.owner.username, fileCount, totalSize];
  } else if (projectResponse.status === 404) {
    return ['not found', 'Unknown', 0, 0];
  } else {
    return ['disconnect', 'Unknown', 0, 0];
  }
} catch (error) {
  return ['disconnect', 'Error', 0, 0];
}
}

// 获取单个文件大小的辅助函数
async function getFileSizeFromGitLab(projectId, filePath, pat) {
const fileUrl = `https://gitlab.com/api/v4/projects/${projectId}/repository/files/${encodeURIComponent(filePath)}?ref=main`;
try {
  const response = await fetch(fileUrl, {
    headers: {
      'PRIVATE-TOKEN': pat
    }
  });

  if (response.status === 200) {
    const data = await response.json();
    const size = atob(data.content).length;
    return size;
  } else {
    console.error(`Error fetching file ${filePath}:`, response.status);
    return 0;
  }
} catch (error) {
  console.error(`Error fetching file ${filePath}:`, error.message);
  return 0;
}
}
*/