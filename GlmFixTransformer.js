// glm_fix_transformer.js
class GlmFixTransformer {
  name = 'glm-fix';

  async transformResponseOut(response, provider) {
    // 如果不是流式响应，直接放行
    if (!response.body) return response;

    const textDecoder = new TextDecoder();
    const textEncoder = new TextEncoder();
    let buffer = '';
    
    // 请求级状态隔离，杜绝并发串扰
    const blockTypeMap = new Map();

    // 使用 TransformStream 替代手动 ReadableStream，原生支持背压(Backpressure)
    const transformStream = new TransformStream({
      transform(chunk, controller) {
        buffer += textDecoder.decode(chunk, { stream: true });
        // 兼容 \r\n 和 \n
        const lines = buffer.split(/\r?\n/);
        // 最后一个元素可能是不完整的行，保留到下一个 chunk
        buffer = lines.pop(); 

        for (const line of lines) {
          this.#processLine(line, controller, blockTypeMap, textEncoder);
        }
      },
      
      flush(controller) {
        // 流结束时，处理 buffer 中可能残留的最后一行（无换行符结尾的情况）
        if (buffer && buffer.trim().length > 0) {
          this.#processLine(buffer, controller, blockTypeMap, textEncoder);
        }
      },

      // 将 processLine 提取为私有方法，保持 transform 逻辑清晰
      '#processLine': undefined 
    });

    // 挂载处理方法（由于 TransformStream 内部无法直接访问外部 this，采用闭包或绑定）
    const processLine = (rawLine, controller, map, encoder) => {
      const trimmed = rawLine.trim();
      
      // 1. 显式处理空行：SSE 协议中空行代表事件结束，必须原样透传以维持事件边界
      if (trimmed === '') {
        controller.enqueue(encoder.encode('\n'));
        return;
      }

      // 2. 非 data: 行（如 event:、id:、retry: 或注释），原样透传
      if (!trimmed.startsWith('data: ')) {
        controller.enqueue(encoder.encode(rawLine + '\n'));
        return;
      }

      const payload = trimmed.slice(6).trim();
      
      // 3. 拦截标准结束符
      if (payload === '[DONE]') {
        controller.enqueue(encoder.encode('data: [DONE]\n\n'));
        return;
      }

      // 4. 解析并校验 JSON
      try {
        const event = JSON.parse(payload);
        const index = event.index;
        const eventType = event.type;

        // 仅对携带有效数字 index 的 content_block 事件进行状态追踪
        if (typeof index === 'number') {
          if (eventType === 'content_block_start') {
            const bType = event.content_block?.type;
            if (bType) map.set(index, bType);
          } 
          else if (eventType === 'content_block_delta') {
            const currentBlockType = map.get(index);
            const deltaType = event.delta?.type;

            // 核心修复：tool_use 块内严禁出现 text_delta
            if (currentBlockType === 'tool_use' && deltaType === 'text_delta') {
              // 丢弃非法事件，不 enqueue
              return; 
            }
          } 
          else if (eventType === 'content_block_stop') {
            map.delete(index);
          }
        }

        // 校验通过，重组标准 SSE 格式转发 (data: {...}\n\n)
        controller.enqueue(encoder.encode(`data: ${payload}\n\n`));
      } catch (e) {
        // 丢弃残损 JSON，防止客户端报 "Unexpected end of JSON input"
        console.warn(`[GlmFixTransformer] 丢弃无效 JSON payload: ${payload.substring(0, 60)}...`);
      }
    };

    // 重新绑定 transform 和 flush 中的处理逻辑
    const safeStream = new TransformStream({
      transform(chunk, controller) {
        buffer += textDecoder.decode(chunk, { stream: true });
        const lines = buffer.split(/\r?\n/);
        buffer = lines.pop();
        for (const line of lines) processLine(line, controller, blockTypeMap, textEncoder);
      },
      flush(controller) {
        if (buffer && buffer.trim().length > 0) {
          processLine(buffer, controller, blockTypeMap, textEncoder);
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
