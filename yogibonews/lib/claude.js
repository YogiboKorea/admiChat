const Anthropic = require('@anthropic-ai/sdk');
const config = require('../config');

// Opus 5 안전 분류기가 요청을 거절하면 서버 측에서 권장 모델로 자동 재시도
const FALLBACK_BETA = 'server-side-fallback-2026-07-01';

let client;

function getClient() {
  if (!client) client = new Anthropic({ apiKey: config.anthropicKey });
  return client;
}

// YOGIBONEWS_CLAUDE_ENABLED=false면 키가 있어도 호출하지 않는다 (로컬 개발 시 토큰 과금 방지)
function hasKey() {
  if (!config.claudeEnabled) return false;
  return Boolean(config.anthropicKey);
}

function model() {
  return config.claudeModel;
}

function readText(message) {
  if (message.stop_reason === 'refusal') {
    throw new Error(`Claude가 요청을 거절했습니다. (${message.stop_details?.category || 'unknown'})`);
  }
  if (message.stop_reason === 'max_tokens') {
    throw new Error('Claude 응답이 max_tokens 한도에서 잘렸습니다.');
  }
  return message.content
    .filter((block) => block.type === 'text')
    .map((block) => block.text)
    .join('');
}

/**
 * Claude 호출 공통 헬퍼.
 * - schema를 주면 구조화 출력(json_schema)으로 받아 파싱된 객체를 반환한다.
 * - content는 문자열 또는 content block 배열(image/document/text).
 */
async function askClaude({ system, content, schema, effort, maxTokens = 16000, stream = false }) {
  if (!hasKey()) throw new Error('Claude 호출이 비활성화되어 있습니다. (ANTHROPIC_API_KEY 미설정 또는 YOGIBONEWS_CLAUDE_ENABLED=false)');

  const params = {
    model: model(),
    max_tokens: maxTokens,
    betas: [FALLBACK_BETA],
    fallbacks: 'default',
    messages: [{ role: 'user', content }],
  };
  // 시스템 프롬프트는 글마다 동일하므로 캐시해서 연속 처리 시 입력 비용을 줄인다
  if (system) params.system = [{ type: 'text', text: system, cache_control: { type: 'ephemeral' } }];

  const outputConfig = {};
  if (effort) outputConfig.effort = effort;
  if (schema) outputConfig.format = { type: 'json_schema', schema };
  if (Object.keys(outputConfig).length) params.output_config = outputConfig;

  const message = stream
    ? await getClient().beta.messages.stream(params).finalMessage()
    : await getClient().beta.messages.create(params);

  const u = message.usage || {};
  console.log(
    `🧾 Claude ${message.model}: 입력 ${u.input_tokens ?? 0} (캐시읽기 ${u.cache_read_input_tokens ?? 0} · 캐시쓰기 ${u.cache_creation_input_tokens ?? 0}) · 출력 ${u.output_tokens ?? 0}`
  );

  const text = readText(message);
  return schema ? JSON.parse(text) : text;
}

module.exports = { askClaude, hasKey };
