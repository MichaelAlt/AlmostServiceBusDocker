<script setup lang="ts">
import { watch, onUnmounted } from 'vue'
import type { MessageInfo } from '../types'
import MessageRow from './MessageRow.vue'
import { useMessages } from '../composables/useMessages'

const props = defineProps<{
  namespace: string
  entity: string
  entityType: 'queue' | 'topic' | null
}>()

const selectedMessage = defineModel<MessageInfo | null>('selectedMessage')

const { messages, connected, refresh, startSse, stopSse } =
  useMessages(() => props.namespace, () => props.entity, () => props.entityType)

watch(() => [props.namespace, props.entity], () => {
  refresh()
  startSse()
}, { immediate: true })

onUnmounted(stopSse)

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
        <button class="btn-danger">Purge</button>
      </div>
      <div v-if="parentPath(entity)" class="parent">{{ parentPath(entity) }}</div>
    </div>

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
  </div>
</template>

<style scoped>
.message-list { width: 380px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.entity-header { padding: 10px 12px; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.title-row { display: flex; align-items: center; justify-content: space-between; }
.name { color: var(--blue); font-weight: 600; font-size: 13px; }
.parent { font-size: 10px; color: var(--text-muted); margin-top: 2px; }
.btn-danger { background: var(--red); color: var(--bg-crust); }
.tabs { display: flex; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.tab { padding: 6px 12px; color: var(--text-muted); font-size: 10px; cursor: pointer; }
.tab.active { border-bottom: 2px solid var(--blue); color: var(--blue); font-weight: 600; }
.live-indicator { padding: 4px 12px; background: var(--bg-mantle); border-bottom: 1px solid var(--border-subtle); font-size: 10px; color: var(--text-muted); }
.live-indicator.connected { color: var(--green); }
.rows { flex: 1; overflow-y: auto; }
.empty { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
