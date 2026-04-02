<script setup lang="ts">
import { ref, watch, onUnmounted, computed } from 'vue'
import type { MessageInfo, SubscriptionInfo } from '../types'
import MessageRow from './MessageRow.vue'
import { useMessages } from '../composables/useMessages'
import { api } from '../api/client'

const props = defineProps<{
  namespace: string
  entity: string
  entityType: 'queue' | 'topic' | null
}>()

const emit = defineEmits<{ selectQueue: [name: string] }>()
const selectedMessage = defineModel<MessageInfo | null>('selectedMessage')

const { messages, connected, refresh, startSse, stopSse } =
  useMessages(() => props.namespace, () => props.entity, () => props.entityType)

watch(() => [props.namespace, props.entity], () => {
  selectedMessage.value = null
  if (props.entityType === 'queue') {
    refresh()
    startSse()
    topicSubscriptions.value = []
  } else if (props.entityType === 'topic') {
    stopSse()
    refreshSubscriptions()
  }
}, { immediate: true })

onUnmounted(stopSse)

// When viewing a topic, fetch its subscriptions
const topicSubscriptions = ref<SubscriptionInfo[]>([])

async function refreshSubscriptions() {
  if (props.entityType !== 'topic') { topicSubscriptions.value = []; return }
  try {
    const data = await api.getEntities(props.namespace)
    const topic = data.topics.find(t => t.name === props.entity)
    topicSubscriptions.value = topic?.subscriptions ?? []
  } catch { topicSubscriptions.value = [] }
}

function shortName(name: string) {
  const idx = name.lastIndexOf('/')
  return idx > 0 ? name.substring(idx + 1) : name
}

function parentPath(name: string) {
  const idx = name.lastIndexOf('/')
  return idx > 0 ? name.substring(0, idx) : null
}
</script>

<template>
  <div class="message-list">
    <div class="entity-header">
      <div class="title-row">
        <span class="name">{{ shortName(entity) }}</span>
        <span class="type-badge">{{ entityType }}</span>
      </div>
      <div v-if="parentPath(entity)" class="parent">{{ parentPath(entity) }}</div>
    </div>

    <!-- Queue view: messages -->
    <template v-if="entityType === 'queue'">
      <div class="tabs">
        <div class="tab active">Messages</div>
        <div class="tab">Dead Letter</div>
        <div class="tab">Properties</div>
      </div>

      <div class="live-indicator" :class="{ connected }">
        {{ connected ? '\u25CF Live' : '\u25CB Disconnected' }}
      </div>

      <div class="rows">
        <MessageRow
          v-for="msg in messages" :key="msg.messageId"
          :message="msg"
          :selected="selectedMessage?.messageId === msg.messageId"
          @select="selectedMessage = msg"
        />
        <div v-if="messages.length === 0" class="empty">
          No messages
        </div>
      </div>
    </template>

    <!-- Topic view: subscriptions -->
    <template v-else-if="entityType === 'topic'">
      <div class="tabs">
        <div class="tab active">Subscriptions</div>
      </div>

      <div class="rows">
        <div
          v-for="sub in topicSubscriptions" :key="sub.name"
          class="sub-item"
          :class="{ clickable: !!sub.forwardTo }"
          @click="sub.forwardTo && emit('selectQueue', sub.forwardTo)"
        >
          <div class="sub-header">
            <span class="sub-name">{{ sub.name }}</span>
            <span v-if="sub.messageCount > 0" class="badge">{{ sub.messageCount }}</span>
          </div>
          <div v-if="sub.forwardTo" class="sub-forward">
            → {{ sub.forwardTo }}
          </div>
          <div class="sub-meta">
            {{ sub.ruleCount }} rule{{ sub.ruleCount !== 1 ? 's' : '' }}
          </div>
        </div>
        <div v-if="topicSubscriptions.length === 0" class="empty">
          No subscriptions
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.message-list { width: 380px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.entity-header { padding: 10px 12px; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.title-row { display: flex; align-items: center; gap: 8px; }
.name { color: var(--blue); font-weight: 600; font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.type-badge { background: var(--bg-surface); color: var(--text-muted); padding: 1px 6px; border-radius: 4px; font-size: 9px; text-transform: uppercase; flex-shrink: 0; }
.parent { font-size: 10px; color: var(--text-muted); margin-top: 2px; }
.tabs { display: flex; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.tab { padding: 6px 12px; color: var(--text-muted); font-size: 10px; cursor: pointer; }
.tab.active { border-bottom: 2px solid var(--blue); color: var(--blue); font-weight: 600; }
.live-indicator { padding: 4px 12px; background: var(--bg-mantle); border-bottom: 1px solid var(--border-subtle); font-size: 10px; color: var(--text-muted); }
.live-indicator.connected { color: var(--green); }
.rows { flex: 1; overflow-y: auto; }
.empty { padding: 20px; text-align: center; color: var(--text-muted); }

.sub-item { padding: 10px 12px; border-bottom: 1px solid var(--border-subtle); }
.sub-item.clickable { cursor: pointer; }
.sub-item.clickable:hover { background: var(--bg-surface); }
.sub-header { display: flex; justify-content: space-between; align-items: center; }
.sub-name { color: var(--text); font-weight: 500; font-size: 12px; }
.badge { background: var(--blue); color: var(--bg-crust); border-radius: 8px; padding: 0 5px; font-size: 9px; }
.sub-forward { color: var(--blue); font-size: 10px; margin-top: 3px; }
.sub-meta { color: var(--text-muted); font-size: 9px; margin-top: 2px; }
</style>
