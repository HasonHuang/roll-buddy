// glm_fix_transformer.js
class GlmFixTransformer {
  name = 'glm-fix';

  async transformResponseOut(response, provider) {
    if (!response.body) return response;

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const encoder = new TextEncoder();
    let buffer = '';
    
    // ✅ 请求级状态隔离，杜绝并发串扰
    const blockTypeMap = new Map();

    const newStream = new ReadableStream({
      start: (controller) => {
        // 抽取单行处理逻辑，保证流结束时的残留数据也能被正确处理
        const processLine = (rawLine) => {
          const trimmed = rawLine.trim();
          
          // 非 data: 行（如 SSE 注释或心跳），原样转发
          if (!trimmed.startsWith('data: ')) {
            controller.enqueue(encoder.encode(rawLine + '\n'));
            return;
          }

          const payload = trimmed.slice(6).trim();
          
          // ✅ 拦截空 payload 和标准结束符
          if (payload === '[DONE]' || payload === '') {
            controller.enqueue(encoder.encode(trimmed + '\n'));
            return;
          }

          try {
            const event = JSON.parse(payload);
            const index = event.index;
            const eventType = event.type;

            // ✅ 仅对携带有效数字 index 的 content_block 事件进行状态追踪
            if (typeof index === 'number') {
              if (eventType === 'content_block_start') {
                const bType = event.content_block?.type;
                if (bType) blockTypeMap.set(index, bType);
              } 
              else if (eventType === 'content_block_delta') {
                const currentBlockType = blockTypeMap.get(index);
                const deltaType = event.delta?.type;

                // 核心修复：tool_use 块内严禁出现 text_delta
                if (currentBlockType === 'tool_use' && deltaType === 'text_delta') {
                  console.warn(`[GlmFixTransformer] 已过滤非法 text_delta: index=${index}`);
                  return; // ✅ 直接丢弃，绝不转发给客户端
                }
              } 
              else if (eventType === 'content_block_stop') {
                blockTypeMap.delete(index);
              }
            }

            // 校验通过，转发原始行
            controller.enqueue(encoder.encode(trimmed + '\n'));
          } catch (e) {
            // ✅ JSON 解析失败说明是残损数据或心跳包，直接丢弃。
            // 转发残损 JSON 是导致客户端报 "Unexpected end of JSON input" 的元凶。
            console.warn(`[GlmFixTransformer] 丢弃无效 JSON: ${payload.substring(0, 60)}...`);
          }
        };

        const pump = () => {
          reader.read().then(({ done, value }) => {
            if (done) {
              // ✅ 流关闭前，处理 buffer 中可能残留的最后一行（无换行符）
              if (buffer.trim().length > 0) {
                processLine(buffer);
              }
              controller.close();
              return;
            }

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split(/\r?\n/);
            buffer = lines.pop() || ''; // 保留未完整的行到下一次

            for (const line of lines) {
              processLine(line);
            }
            pump();
          }).catch(err => controller.error(err));
        };
        pump();
      }
    });

    // ✅ 清理定长头部，防止客户端因字节数不匹配提前断流
    const newHeaders = new Headers(response.headers);
    newHeaders.delete('Content-Length');
    newHeaders.delete('Content-Encoding');

    return new Response(newStream, {
      status: response.status,
      statusText: response.statusText,
      headers: newHeaders,
    });
  }
}

module.exports = GlmFixTransformer;
