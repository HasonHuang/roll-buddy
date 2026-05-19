/**
 * GLM-4 流式响应修复转换器
 * 解决 GLM-4 在 tool_use 块中错误穿插 text_delta (如换行符) 导致 Anthropic SDK 报错的问题
 * 同时修复残损 JSON 导致的 "Unexpected end of JSON input" 问题
 */
class GlmFixTransformer {
  name = 'glm-fix';

  constructor(options = {}) {
    this.options = options;
  }

  /**
   * 处理请求入参（当前无需修改，直接透传）
   * @param {object} request - 统一格式的 LLM 请求体
   * @param {object} provider - 当前提供商配置
   */
  async transformRequestIn(request, provider) {
    return request;
  }

  /**
   * 处理响应出流（核心修复逻辑）
   * @param {Response} response - 原始 Fetch Response 对象
   * @param {object} provider - 当前提供商配置
   */
  async transformResponseOut(response, provider) {
    // 如果不是流式响应，直接放行
    if (!response.body) {
      return response;
    }

    const textDecoder = new TextDecoder();
    const textEncoder = new TextEncoder();
    let buffer = '';
    
    // 请求级状态隔离，记录每个 index 对应的 block 类型
    const blockTypeMap = new Map();

    // 单行 SSE 处理逻辑
    const processLine = (rawLine, controller) => {
      const trimmed = rawLine.trim();
      
      // 1. 显式处理空行：SSE 协议中空行代表事件结束，必须透传以维持事件边界
      if (trimmed === '') {
        controller.enqueue(textEncoder.encode('\n'));
        return;
      }

      // 2. 非 data: 行（如 event:、id: 或注释），原样透传
      if (!trimmed.startsWith('data: ')) {
        controller.enqueue(textEncoder.encode(rawLine + '\n'));
        return;
      }

      const payload = trimmed.slice(6).trim();
      
      // 3. 拦截标准结束符
      if (payload === '[DONE]') {
        controller.enqueue(textEncoder.encode('data: [DONE]\n\n'));
        return;
      }
      
      // 4. 拦截空 payload
      if (payload === '') {
        return;
      }

      // 5. 解析并校验 JSON
      try {
        const event = JSON.parse(payload);
        const index = event.index;
        const eventType = event.type;

        // 仅对携带有效数字 index 的 content_block 事件进行状态追踪
        if (typeof index === 'number') {
          if (eventType === 'content_block_start') {
            const bType = event.content_block?.type;
            if (bType) blockTypeMap.set(index, bType);
          } 
          else if (eventType === 'content_block_delta') {
            const currentBlockType = blockTypeMap.get(index);
            const deltaType = event.delta?.type;

            // 核心修复：tool_use 块内严禁出现 text_delta (如 \n)
            if (currentBlockType === 'tool_use' && deltaType === 'text_delta') {
              // 丢弃非法事件，不 enqueue
              return; 
            }
          } 
          else if (eventType === 'content_block_stop') {
            blockTypeMap.delete(index);
          }
        }

        // 校验通过，重组标准 SSE 格式转发
        controller.enqueue(textEncoder.encode(`data: ${payload}\n\n`));
      } catch (e) {
        // 丢弃残损 JSON，防止客户端报 "Unexpected end of JSON input"
        // 如需调试可取消下方注释
        // console.warn(`[GlmFixTransformer] 丢弃无效 JSON payload: ${payload.substring(0, 60)}...`);
      }
    };

    // 使用 TransformStream 原生支持背压(Backpressure)，防止内存溢出
    const safeStream = new TransformStream({
      transform(chunk, controller) {
        buffer += textDecoder.decode(chunk, { stream: true });
        // 兼容 \r\n 和 \n
        const lines = buffer.split(/\r?\n/);
        // 保留最后一个可能不完整的行到下一个 chunk
        buffer = lines.pop() || ''; 
        
        for (const line of lines) {
          processLine(line, controller);
        }
      },
      flush(controller) {
        // 流结束时，处理 buffer 中可能残留的最后一行
        if (buffer && buffer.trim().length > 0) {
          processLine(buffer, controller);
        }
      }
    });

    // 安全克隆 Headers 并清理定长/编码头
    const newHeaders = new Headers();
    for (const [key, value] of response.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (lowerKey !== 'content-length' && lowerKey !== 'content-encoding') {
        newHeaders.set(key, value);
      }
    }
    // 确保 SSE 必要的头部存在
    newHeaders.set('Cache-Control', 'no-cache');
    newHeaders.set('Connection', 'keep-alive');

    return new Response(response.body.pipeThrough(safeStream), {
      status: response.status,
      statusText: response.statusText,
      headers: newHeaders,
    });
  }
}

module.exports = GlmFixTransformer;
