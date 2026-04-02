import { ref, computed, reactive, watch } from 'vue'
import type { EntityOverview, QueueInfo, TopicInfo, EntityGroup, NamespaceInfo } from '../types'
import { api } from '../api/client'
import { connectSse } from '../api/sse'

export function useEntities(selectedNamespace: () => string) {
  const namespaces = ref<NamespaceInfo[]>([])
  const entities = ref<EntityOverview | null>(null)
  const loading = ref(false)
  const filter = ref('')
  const collapsedGroups = reactive(new Set<string>())

  // Namespace list polls (infrequent, lightweight)
  let nsInterval: ReturnType<typeof setInterval>

  async function refreshNamespaces() {
    try { namespaces.value = await api.getNamespaces() } catch {}
  }

  async function refreshEntities() {
    const ns = selectedNamespace()
    if (!ns) return
    loading.value = true
    try { entities.value = await api.getEntities(ns) } catch {}
    finally { loading.value = false }
  }

  // SSE for real-time entity updates — when a message flows through
  // the namespace, refresh the entity list to pick up new counts/entities
  let entitySse: EventSource | null = null

  function startEntitySse() {
    entitySse?.close()
    const ns = selectedNamespace()
    if (!ns) return

    entitySse = connectSse(ns, undefined, () => {
      // Any event in this namespace = refresh entity counts
      refreshEntities()
    })
  }

  function startPolling() {
    refreshNamespaces()
    refreshEntities()
    startEntitySse()
    nsInterval = setInterval(refreshNamespaces, 5000)
  }

  function stopPolling() {
    clearInterval(nsInterval)
    entitySse?.close()
    entitySse = null
  }

  // When namespace changes, reload entities and reconnect SSE
  function onNamespaceChange() {
    entities.value = null
    refreshEntities()
    startEntitySse()
  }

  function toggleGroup(prefix: string) {
    if (collapsedGroups.has(prefix)) {
      collapsedGroups.delete(prefix)
    } else {
      collapsedGroups.add(prefix)
    }
  }

  function isCollapsed(prefix: string) {
    return collapsedGroups.has(prefix)
  }

  const topicGroups = computed<EntityGroup[]>(() => {
    if (!entities.value) return []
    const groups = new Map<string, TopicInfo[]>()
    for (const topic of entities.value.topics) {
      const slashIdx = topic.name.lastIndexOf('/')
      const prefix = slashIdx > 0 ? topic.name.substring(0, slashIdx) : ''
      if (!groups.has(prefix)) groups.set(prefix, [])
      groups.get(prefix)!.push(topic)
    }
    return Array.from(groups.entries()).map(([prefix, topics]) => ({
      prefix: prefix || '(ungrouped)',
      topics,
      collapsed: collapsedGroups.has(prefix || '(ungrouped)'),
    }))
  })

  const filteredQueues = computed<QueueInfo[]>(() => {
    if (!entities.value) return []
    const f = filter.value.toLowerCase()
    return f ? entities.value.queues.filter(q => q.name.toLowerCase().includes(f)) : entities.value.queues
  })

  return { namespaces, entities, loading, filter, topicGroups, filteredQueues, toggleGroup, isCollapsed, startPolling, stopPolling, onNamespaceChange, refreshEntities }
}
