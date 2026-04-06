import type { MessageEvent as MsgEvent } from '../types'

export function connectSse(
  ns?: string,
  entity?: string,
  onEvent?: (evt: MsgEvent) => void,
): EventSource {
  const params = new URLSearchParams()
  if (ns) params.set('ns', ns)
  if (entity) params.set('entity', entity)

  const url = `/api/dashboard/events?${params}`
  const source = new EventSource(url)

  source.onmessage = (e) => {
    try {
      const parsed: MsgEvent = JSON.parse(e.data)
      onEvent?.(parsed)
    } catch { /* ignore parse errors */ }
  }

  return source
}
