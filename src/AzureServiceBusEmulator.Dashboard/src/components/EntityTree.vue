<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import NamespaceTabs from './NamespaceTabs.vue'
import { useEntities } from '../composables/useEntities'

const ns = defineModel<string>('namespace', { required: true })
const entity = defineModel<string | null>('entity', { required: true })
const entityType = defineModel<'queue' | 'topic' | null>('entityType', { required: true })
const emit = defineEmits<{ select: [] }>()

const { namespaces, filter, topicGroups, filteredQueues, toggleGroup, isCollapsed, startPolling, stopPolling } =
  useEntities(() => ns.value)

onMounted(startPolling)
onUnmounted(stopPolling)

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
    <NamespaceTabs :namespaces="namespaces" v-model:selected="ns" />

    <div class="search">
      <input v-model="filter" placeholder="Filter entities..." />
    </div>

    <div class="tree">
      <div class="section-header">Queues</div>
      <div
        v-for="q in filteredQueues" :key="q.name"
        class="entity-row" :class="{ selected: entity === q.name }"
        @click="selectEntity(q.name, 'queue')"
      >
        {{ q.name }}
        <span v-if="q.deadLetterCount > 0" class="badge-red">{{ q.deadLetterCount }}</span>
      </div>

      <div class="section-header">Topics</div>
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
            </div>
          </template>
        </template>
      </template>
    </div>

    <div class="footer">
      {{ filteredQueues.length }} queues &middot;
      {{ topicGroups.reduce((n, g) => n + g.topics.length, 0) }} topics
    </div>
  </div>
</template>

<style scoped>
.sidebar { width: 300px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.search { padding: 8px; }
.search input { width: 100%; background: var(--bg-surface); border: none; border-radius: 4px; padding: 6px 10px; color: var(--text); font-size: 11px; outline: none; }
.tree { flex: 1; overflow-y: auto; padding: 4px 8px; }
.section-header { color: var(--text-muted); font-size: 10px; text-transform: uppercase; padding: 8px 4px 4px; font-weight: 600; letter-spacing: 0.5px; }
.group-header { padding: 4px 4px 4px 12px; color: var(--text-dim); cursor: pointer; font-size: 11px; user-select: none; }
.group-header:hover { color: var(--text); }
.chevron { color: var(--text-muted); margin-right: 4px; }
.entity-row { padding: 4px 4px 4px 12px; cursor: pointer; border-radius: 4px; font-size: 11px; }
.entity-row.indent { padding-left: 28px; }
.entity-row:hover { background: var(--bg-surface); }
.entity-row.selected { background: var(--bg-surface); color: var(--blue); }
.sub-row { padding: 3px 4px 3px 40px; color: var(--text-dim); font-size: 10px; }
.sub-row.clickable { cursor: pointer; border-radius: 4px; }
.sub-row.clickable:hover { background: var(--bg-surface); color: var(--text); }
.sub-row.selected { color: var(--blue); }
.forward-to { color: var(--text-muted); font-size: 9px; margin-left: 4px; }
.badge-red { background: var(--red); color: var(--bg-crust); border-radius: 8px; padding: 0 6px; font-size: 10px; margin-left: 4px; }
.footer { border-top: 1px solid var(--border); padding: 8px 12px; background: var(--bg-mantle); font-size: 10px; color: var(--text-muted); }
</style>
