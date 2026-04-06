<script setup lang="ts">
import { ref } from 'vue'
import type { MessageInfo } from '../types'
import JsonViewer from './JsonViewer.vue'

const props = defineProps<{ message: MessageInfo }>()
const activeTab = ref<'body' | 'appProps' | 'sysProps'>('body')
</script>

<template>
  <div class="detail">
    <div class="detail-header">
      <span class="msg-id">{{ message.messageId }}</span>
    </div>

    <div class="metadata">
      <div class="meta-row">
        <span class="label">Message ID</span>
        <span class="value mono">{{ message.messageId }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Enqueued</span>
        <span class="value">{{ new Date(message.enqueuedTimeUtc).toLocaleString() }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Content Type</span>
        <span class="value">{{ message.contentType ?? '\u2014' }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Correlation ID</span>
        <span class="value mono">{{ message.correlationId ?? '\u2014' }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Delivery Count</span>
        <span class="value">{{ message.deliveryCount }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Sequence No.</span>
        <span class="value">{{ message.sequenceNumber }}</span>
      </div>
    </div>

    <div class="tabs">
      <div class="tab" :class="{ active: activeTab === 'body' }" @click="activeTab = 'body'">Body</div>
      <div class="tab" :class="{ active: activeTab === 'appProps' }" @click="activeTab = 'appProps'">
        App Properties
        <span v-if="message.applicationProperties" class="count">
          {{ Object.keys(message.applicationProperties).length }}
        </span>
      </div>
      <div class="tab" :class="{ active: activeTab === 'sysProps' }" @click="activeTab = 'sysProps'">System</div>
    </div>

    <div class="tab-content">
      <div v-if="activeTab === 'body'" class="body-viewer">
        <JsonViewer v-if="message.bodyText" :json="message.bodyText" />
        <div v-else class="empty">No body</div>
      </div>

      <div v-if="activeTab === 'appProps'" class="props-viewer">
        <template v-if="message.applicationProperties">
          <div v-for="(val, key) in message.applicationProperties" :key="key" class="prop-row">
            <span class="prop-key">{{ key }}</span>
            <span class="prop-val">{{ JSON.stringify(val) }}</span>
          </div>
        </template>
        <div v-else class="empty">No application properties</div>
      </div>

      <div v-if="activeTab === 'sysProps'" class="props-viewer">
        <div class="prop-row">
          <span class="prop-key">SequenceNumber</span>
          <span class="prop-val">{{ message.sequenceNumber }}</span>
        </div>
        <div class="prop-row">
          <span class="prop-key">DeliveryCount</span>
          <span class="prop-val">{{ message.deliveryCount }}</span>
        </div>
        <div class="prop-row">
          <span class="prop-key">EnqueuedTimeUtc</span>
          <span class="prop-val">{{ message.enqueuedTimeUtc }}</span>
        </div>
        <div v-if="message.subject" class="prop-row">
          <span class="prop-key">Subject</span>
          <span class="prop-val">{{ message.subject }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.detail { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
.detail-header { padding: 10px 16px; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.msg-id { color: var(--text); font-weight: 600; font-size: 13px; }
.metadata { padding: 10px 16px; border-bottom: 1px solid var(--border); font-size: 11px; }
.meta-row { display: grid; grid-template-columns: 120px 1fr; gap: 4px 12px; padding: 2px 0; }
.label { color: var(--text-muted); }
.value { color: var(--text); }
.value.mono { font-family: monospace; font-size: 10px; }
.tabs { display: flex; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.tab { padding: 6px 12px; color: var(--text-muted); font-size: 10px; cursor: pointer; }
.tab.active { border-bottom: 2px solid var(--blue); color: var(--blue); font-weight: 600; }
.count { background: var(--bg-surface); padding: 0 4px; border-radius: 3px; font-size: 9px; margin-left: 4px; }
.tab-content { flex: 1; overflow-y: auto; background: var(--bg-crust); }
.body-viewer { padding: 12px 16px; min-height: 100%; }
.props-viewer { padding: 12px 16px; }
.prop-row { display: grid; grid-template-columns: 200px 1fr; gap: 4px 12px; padding: 4px 0; border-bottom: 1px solid var(--border-subtle); font-size: 11px; }
.prop-key { color: var(--blue); font-family: monospace; }
.prop-val { color: var(--text); font-family: monospace; word-break: break-all; }
.empty { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
