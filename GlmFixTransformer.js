// glm_fix_transformer.js
class GlmFixTransformer {
  name = 'glm-fix';

  async transformResponseOut(response, provider) {
    if (!response.body) return response;

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const encoder = new TextEncoder();
    let buffer = '';

    // ✅ 修复1：将状态移至请求/流作用域，彻底杜绝并发串扰
    const blockTypeMap = new Map();

    const newStream = new ReadableStream({
      start: (controller) => {
        const pump = () => {
          reader.read().then(({ done, value }) => {
            if (done) {
              controller.close();
              return;
            }

            buffer += decoder.decode(value, { stream: true });
            // ✅ 修复5：兼容 \r\n 和 \n
            const lines = buffer.split(/\r?\n/);
            buffer = lines.pop() || '';

            for (const line of lines) {
              const trimmedLine = line.trim();
              if (!trimmedLine.startsWith('data: ')) {
                controller.enqueue(encoder.encode(line + '\n'));
                continue;
              }

              const jsonStr = trimmedLine.slice(6);
              if (jsonStr === '[DONE]') {
                controller.enqueue(encoder.encode(line + '\n'));
                continue;
              }

              try {
                const event = JSON.parse(jsonStr);
                const index = event.index;
                const eventType = event.type;

                // ✅ 修复2：仅对有效数字 index 进行状态追踪
                if (typeof index === 'number') {
                  if (eventType === 'content_block_start') {
                    // ✅ 修复3：安全访问深层属性
                    const bType = event.content_block?.type;
                    if (bType) blockTypeMap.set(index, bType);
                  } 
                  else if (eventType === 'content_block_delta') {
                    const currentBlockType = blockTypeMap.get(index);
                    const deltaType = event.delta?.type;

                    // 核心过滤：tool_use 块内严禁出现 text_delta
                    if (currentBlockType === 'tool_use' && deltaType === 'text_delta') {
                      console.warn(`[GlmFixTransformer] 已过滤非法 text_delta: index=${index}`);
                      continue; // 跳过转发
                    }
                  } 
                  else if (eventType === 'content_block_stop') {
                    blockTypeMap.delete(index);
                  }
                }

                // 转发合法事件
                controller.enqueue(encoder.encode(line + '\n'));
              } catch (e) {
                // JSON 解析失败，原样转发保活
                controller.enqueue(encoder.encode(line + '\n'));
              }
            }
            pump(); // 继续读取下一块
          }).catch(err => controller.error(err));
        };
        pump();
      }
    });

    // ✅ 修复4：清理会导致流截断的头部
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
