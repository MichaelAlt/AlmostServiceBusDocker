import { ref, computed, reactive } from 'vue'
import type { EntityOverview, QueueInfo, TopicInfo, EntityGroup, NamespaceInfo } from '../types'
import { api } from '../api/client'

export function useEntities(selectedNamespace: () => string) {
  const namespaces = ref<NamespaceInfo[]>([])
  const entities = ref<EntityOverview | null>(null)
  const loading = ref(false)
  const filter = ref('')
  const showAll = ref(false)
  const collapsedGroups = reactive(new Set<string>())

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

  function startPolling() {
    refreshNamespaces()
    refreshEntities()
    nsInterval = setInterval(() => {
      refreshNamespaces()
      refreshEntities()
    }, 5000)
  }

  function stopPolling() {
    clearInterval(nsInterval)
  }

  function onNamespaceChange() {
    entities.value = null
    refreshEntities()
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

  function hasActivity(q: QueueInfo): boolean {
    return q.totalMessageCount > 0 || q.deadLetterCount > 0
  }

  function topicHasMessages(t: TopicInfo): boolean {
    return t.subscriptions.some(s => s.messageCount > 0)
  }

  const filteredQueues = computed<QueueInfo[]>(() => {
    if (!entities.value) return []
    let queues = entities.value.queues
    const f = filter.value.toLowerCase()
    if (f) queues = queues.filter(q => q.name.toLowerCase().includes(f))
    if (!showAll.value) queues = queues.filter(hasActivity)
    return queues
  })

  const topicGroups = computed<EntityGroup[]>(() => {
    if (!entities.value) return []
    let topics = entities.value.topics
    if (!showAll.value) topics = topics.filter(topicHasMessages)

    const groups = new Map<string, TopicInfo[]>()
    for (const topic of topics) {
      const slashIdx = topic.name.lastIndexOf('/')
      const prefix = slashIdx > 0 ? topic.name.substring(0, slashIdx) : ''
      if (!groups.has(prefix)) groups.set(prefix, [])
      groups.get(prefix)!.push(topic)
    }
    return Array.from(groups.entries()).map(([prefix, t]) => ({
      prefix: prefix || '(ungrouped)',
      topics: t,
      collapsed: collapsedGroups.has(prefix || '(ungrouped)'),
    }))
  })

  const totalQueues = computed(() => entities.value?.queues.length ?? 0)
  const totalTopics = computed(() => entities.value?.topics.length ?? 0)

  return {
    namespaces, entities, loading, filter, showAll, topicGroups, filteredQueues,
    totalQueues, totalTopics,
    toggleGroup, isCollapsed, startPolling, stopPolling, onNamespaceChange, refreshEntities,
  }
}
