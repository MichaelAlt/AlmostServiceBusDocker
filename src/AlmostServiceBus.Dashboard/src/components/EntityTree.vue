<script setup lang="ts">
import { inject, onMounted, onUnmounted, watch } from 'vue'
import NamespaceSelector from './NamespaceSelector.vue'
import { useEntities } from '../composables/useEntities'
import { sseKey } from '../composables/useNamespaceSse'

const ns = defineModel<string>('namespace', { required: true })
const entity = defineModel<string | null>('entity', { required: true })
const entityType = defineModel<'queue' | 'topic' | null>('entityType', { required: true })
const emit = defineEmits<{ select: [] }>()

const sse = inject(sseKey)!

const {
  namespaces, loading, filter, showAll, topicGroups, filteredQueues,
  totalQueues, totalTopics,
  toggleGroup, isCollapsed, start, stop, onNamespaceChange,
} = useEntities(() => ns.value, sse)

onMounted(start)
onUnmounted(stop)

watch(ns, () => {
  entity.value = null
  entityType.value = null
  onNamespaceChange()
})

function selectEntity(name: string, type: 'queue' | 'topic') {
  entity.value = name
  entityType.value = type
  emit('select')
}

function shortName(fullName: string) {
  const idx = fullName.lastIndexOf('/')
  return idx > 0 ? fullName.substring(idx + 1) : fullName
}
</script>

<template>
  <div class="sidebar">
    <NamespaceSelector :namespaces="namespaces" v-model:selected="ns" />

    <div class="search">
      <input v-model="filter" placeholder="Filter entities..." />
    </div>

    <div class="toggle-row">
      <label class="toggle">
        <input type="checkbox" v-model="showAll" />
        <span>Show all entities</span>
      </label>
    </div>

    <div class="tree">
      <div v-if="loading" class="loading">Loading entities...</div>
      <template v-else>

      <div class="section-header">{{ showAll ? 'All Queues' : 'Active Queues' }}</div>
      <div v-if="!showAll && filteredQueues.length === 0" class="empty-hint">
        No queues with messages
      </div>
      <div
        v-for="q in filteredQueues" :key="q.name"
        class="entity-row" :class="{ selected: entity === q.name }"
        @click="selectEntity(q.name, 'queue')"
      >
        <span class="entity-name">{{ q.name }}</span>
        <span class="entity-badges">
          <span v-if="q.totalMessageCount - q.consumedCount - q.deadLetterCount > 0" class="badge">{{ q.totalMessageCount - q.consumedCount - q.deadLetterCount }}</span>
          <span v-if="q.consumedCount > 0" class="badge-green">{{ q.consumedCount }}</span>
          <span v-if="q.deadLetterCount > 0" class="badge-red">{{ q.deadLetterCount }}</span>
        </span>
      </div>

      <div class="section-header">{{ showAll ? 'All Topics' : 'Active Topics' }}</div>
      <div v-if="!showAll && topicGroups.length === 0" class="empty-hint">
        No topics with messages
      </div>
      <template v-for="group in topicGroups" :key="group.prefix">
        <div class="group-header" @click="toggleGroup(group.prefix)">
          <span class="chevron">{{ isCollapsed(group.prefix) ? '▸' : '▾' }}</span>
          {{ group.prefix }}
        </div>
        <template v-if="!isCollapsed(group.prefix)">
          <template v-for="t in group.topics" :key="t.name">
            <div
              class="entity-row indent" :class="{ selected: entity === t.name }"
              @click="selectEntity(t.name, 'topic')"
            >
              {{ shortName(t.name) }}
            </div>
            <div
              v-for="s in t.subscriptions" :key="s.name"
              class="sub-row clickable"
              :class="{ selected: entity === s.forwardTo }"
              @click="s.forwardTo && selectEntity(s.forwardTo, 'queue')"
            >
              ↳ {{ s.name }}
              <span v-if="s.forwardTo" class="forward-to">→ {{ s.forwardTo }}</span>
              <span v-if="s.messageCount > 0" class="badge-sm">{{ s.messageCount }}</span>
            </div>
          </template>
        </template>
      </template>

      </template>
    </div>

    <div class="footer">
      {{ filteredQueues.length }}<span v-if="!showAll">/{{ totalQueues }}</span> queues ·
      {{ topicGroups.reduce((n, g) => n + g.topics.length, 0) }}<span v-if="!showAll">/{{ totalTopics }}</span> topics
    </div>
  </div>
</template>

<style scoped>
.sidebar { width: 300px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.search { padding: 8px 8px 4px; }
.search input { width: 100%; background: var(--bg-surface); border: none; border-radius: 4px; padding: 6px 10px; color: var(--text); font-size: 11px; outline: none; }
.toggle-row { padding: 0 8px 4px; }
.toggle { display: flex; align-items: center; gap: 6px; font-size: 10px; color: var(--text-muted); cursor: pointer; user-select: none; }
.toggle input { accent-color: var(--blue); width: 12px; height: 12px; }
.tree { flex: 1; overflow-y: auto; padding: 4px 8px; }
.section-header { color: var(--text-muted); font-size: 10px; text-transform: uppercase; padding: 8px 4px 4px; font-weight: 600; letter-spacing: 0.5px; }
.empty-hint { padding: 4px 12px; font-size: 10px; color: var(--text-muted); font-style: italic; }
.group-header { padding: 4px 4px 4px 12px; color: var(--text-dim); cursor: pointer; font-size: 11px; user-select: none; }
.group-header:hover { color: var(--text); }
.chevron { color: var(--text-muted); margin-right: 4px; }
.entity-row { padding: 4px 4px 4px 12px; cursor: pointer; border-radius: 4px; font-size: 11px; display: flex; justify-content: space-between; align-items: center; }
.entity-row.indent { padding-left: 28px; }
.entity-row:hover { background: var(--bg-surface); }
.entity-row.selected { background: var(--bg-surface); color: var(--blue); }
.entity-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
.entity-badges { display: flex; gap: 3px; flex-shrink: 0; }
.badge { background: var(--blue); color: var(--bg-crust); border-radius: 8px; padding: 0 5px; font-size: 9px; }
.badge-green { background: var(--green); color: var(--bg-crust); border-radius: 8px; padding: 0 5px; font-size: 9px; }
.badge-red { background: var(--red); color: var(--bg-crust); border-radius: 8px; padding: 0 5px; font-size: 9px; }
.badge-sm { background: var(--bg-surface); color: var(--text-dim); border-radius: 8px; padding: 0 4px; font-size: 9px; margin-left: 4px; }
.sub-row { padding: 3px 4px 3px 40px; color: var(--text-dim); font-size: 10px; }
.sub-row.clickable { cursor: pointer; border-radius: 4px; }
.sub-row.clickable:hover { background: var(--bg-surface); color: var(--text); }
.sub-row.selected { color: var(--blue); }
.forward-to { color: var(--text-muted); font-size: 9px; margin-left: 4px; }
.loading { padding: 20px; text-align: center; color: var(--text-muted); font-size: 11px; animation: pulse 1.5s ease-in-out infinite; }
@keyframes pulse { 0%, 100% { opacity: 0.4; } 50% { opacity: 1; } }
.footer { border-top: 1px solid var(--border); padding: 8px 12px; background: var(--bg-mantle); font-size: 10px; color: var(--text-muted); }
</style>
