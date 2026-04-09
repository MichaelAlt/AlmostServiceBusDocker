<script setup lang="ts">
import { ref, inject, watch, onUnmounted, computed } from 'vue'
import type { MessageInfo, SubscriptionInfo } from '../types'
import MessageRow from './MessageRow.vue'
import { useMessages } from '../composables/useMessages'
import { sseKey } from '../composables/useNamespaceSse'
import { api } from '../api/client'

const props = defineProps<{
  namespace: string
  entity: string
  entityType: 'queue' | 'topic' | null
}>()

const emit = defineEmits<{ selectQueue: [name: string] }>()
const selectedMessage = defineModel<MessageInfo | null>('selectedMessage')

const sse = inject(sseKey)!

const { messages, deadLetterMessages, connected, refresh, refreshDeadLetter, startListening, stopListening } =
  useMessages(() => props.namespace, () => props.entity, () => props.entityType, sse)

const activeTab = ref<'messages' | 'deadletter' | 'properties'>('messages')
const hideConsumed = ref(false)
const visibleMessages = computed(() =>
  hideConsumed.value ? messages.value.filter(m => m.state !== 'Consumed' && m.state !== 'DeadLettered') : messages.value
)

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

function switchTab(tab: 'messages' | 'deadletter' | 'properties') {
  activeTab.value = tab
  selectedMessage.value = null
  if (tab === 'deadletter') refreshDeadLetter()
}

watch(() => [props.namespace, props.entity, props.entityType], () => {
  selectedMessage.value = null
  activeTab.value = 'messages'
  messages.value = []
  deadLetterMessages.value = []
  if (props.entityType === 'queue') {
    refresh()
    startListening()
    topicSubscriptions.value = []
  } else if (props.entityType === 'topic') {
    stopListening()
    refreshSubscriptions()
  }
}, { immediate: true })

onUnmounted(stopListening)

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
        <div class="tab" :class="{ active: activeTab === 'messages' }" @click="switchTab('messages')">Messages</div>
        <div class="tab" :class="{ active: activeTab === 'deadletter' }" @click="switchTab('deadletter')">Dead Letter</div>
        <div class="tab" :class="{ active: activeTab === 'properties' }" @click="switchTab('properties')">Properties</div>
      </div>

      <!-- Messages tab -->
      <template v-if="activeTab === 'messages'">
        <div class="list-toolbar">
          <span class="live-indicator" :class="{ connected }">
            {{ connected ? '\u25CF Live' : '\u25CB Disconnected' }}
          </span>
          <label class="toggle">
            <input type="checkbox" v-model="hideConsumed" />
            <span>Hide consumed</span>
          </label>
        </div>

        <div class="rows">
          <MessageRow
            v-for="msg in visibleMessages" :key="msg.messageId"
            :message="msg"
            :selected="selectedMessage?.messageId === msg.messageId"
            @select="selectedMessage = msg"
          />
          <div v-if="visibleMessages.length === 0" class="empty">
            No messages
          </div>
        </div>
      </template>

      <!-- Dead Letter tab -->
      <template v-if="activeTab === 'deadletter'">
        <div class="rows">
          <MessageRow
            v-for="msg in deadLetterMessages" :key="msg.messageId"
            :message="msg"
            :selected="selectedMessage?.messageId === msg.messageId"
            @select="selectedMessage = msg"
          />
          <div v-if="deadLetterMessages.length === 0" class="empty">
            No dead-letter messages
          </div>
        </div>
      </template>

      <!-- Properties tab (placeholder) -->
      <template v-if="activeTab === 'properties'">
        <div class="empty">Queue properties not yet implemented</div>
      </template>
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
.list-toolbar { display: flex; align-items: center; justify-content: space-between; padding: 4px 12px; background: var(--bg-mantle); border-bottom: 1px solid var(--border-subtle); }
.live-indicator { font-size: 10px; color: var(--text-muted); }
.live-indicator.connected { color: var(--green); }
.toggle { display: flex; align-items: center; gap: 5px; font-size: 10px; color: var(--text-muted); cursor: pointer; user-select: none; }
.toggle input { accent-color: var(--blue); width: 12px; height: 12px; }
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
