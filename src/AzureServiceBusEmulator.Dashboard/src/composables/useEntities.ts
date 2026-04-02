import { ref, computed, reactive } from 'vue'
import type { EntityOverview, QueueInfo, TopicInfo, EntityGroup, NamespaceInfo } from '../types'
import { api } from '../api/client'

export function useEntities(selectedNamespace: () => string) {
  const namespaces = ref<NamespaceInfo[]>([])
  const entities = ref<EntityOverview | null>(null)
  const filter = ref('')
  const collapsedGroups = reactive(new Set<string>())

  async function refreshNamespaces() {
    try { namespaces.value = await api.getNamespaces() } catch { /* ignore */ }
  }

  async function refreshEntities() {
    try { entities.value = await api.getEntities(selectedNamespace()) } catch { /* ignore */ }
  }

  let interval: ReturnType<typeof setInterval>
  function startPolling() {
    refreshNamespaces()
    refreshEntities()
    interval = setInterval(() => {
      refreshNamespaces()
      refreshEntities()
    }, 3000)
  }
  function stopPolling() { clearInterval(interval) }

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

  return { namespaces, entities, filter, topicGroups, filteredQueues, toggleGroup, isCollapsed, startPolling, stopPolling, refreshEntities }
}
