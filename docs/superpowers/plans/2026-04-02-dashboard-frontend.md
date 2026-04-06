# Dashboard Frontend — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Vue 3 diagnostic dashboard served from the emulator's HTTP server, with a three-panel layout for browsing entities, messages, and message details in real time.

**Architecture:** Vue 3 + TypeScript SPA using Composition API with `<script setup>`. Vite for dev/build. SSE for real-time updates. Served as static files from the emulator's Kestrel HTTP server via Vite.AspNetCore in development.

**Tech Stack:** Vue 3, TypeScript, Vite, Vite.AspNetCore, EventSource API

**Spec:** `docs/superpowers/specs/2026-04-02-dashboard-ui-design.md`

**Depends on:** Dashboard Backend API (plan: `2026-04-02-dashboard-backend.md`)

---

### Task 1: Scaffold Vue project

**Files:**
- Create: `src/AlmostServiceBus.Dashboard/` (entire directory)

- [ ] **Step 1: Create Vue project with Vite**

```bash
cd src
npm create vue@latest AlmostServiceBus.Dashboard -- --typescript --router=false --pinia=false --vitest=false --e2e=false --eslint=false --prettier=false
cd AlmostServiceBus.Dashboard
npm install
```

- [ ] **Step 2: Clean up scaffolded files**

Remove the default App.vue content, HelloWorld component, and asset files. Keep the project structure.

- [ ] **Step 3: Configure Vite for proxy to emulator API**

```typescript
// src/AlmostServiceBus.Dashboard/vite.config.ts
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:5300',  // Emulator's internal HTTP port
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: '../AlmostServiceBus.Host/wwwroot',
    emptyOutDir: true,
  },
})
```

- [ ] **Step 4: Verify dev server starts**

Run: `npm run dev`

Expected: Vite dev server starts on localhost:5173.

- [ ] **Step 5: Commit**

```bash
git add src/AlmostServiceBus.Dashboard/
git commit -m "feat: scaffold Vue dashboard project"
```

---

### Task 2: TypeScript types and API client

**Files:**
- Create: `src/AlmostServiceBus.Dashboard/src/types/index.ts`
- Create: `src/AlmostServiceBus.Dashboard/src/api/client.ts`
- Create: `src/AlmostServiceBus.Dashboard/src/api/sse.ts`

- [ ] **Step 1: Define TypeScript interfaces**

```typescript
// src/AlmostServiceBus.Dashboard/src/types/index.ts

export interface NamespaceInfo {
  name: string
  queueCount: number
  topicCount: number
}

export interface EntityOverview {
  queues: QueueInfo[]
  topics: TopicInfo[]
}

export interface QueueInfo {
  name: string
  messageCount: number
  deadLetterCount: number
  maxDeliveryCount: number
  forwardTo: string | null
}

export interface TopicInfo {
  name: string
  subscriptions: SubscriptionInfo[]
}

export interface SubscriptionInfo {
  name: string
  forwardTo: string | null
  messageCount: number
  ruleCount: number
}

export interface MessageInfo {
  messageId: string
  sequenceNumber: number
  contentType: string | null
  correlationId: string | null
  deliveryCount: number
  enqueuedTimeUtc: string
  subject: string | null
  applicationProperties: Record<string, unknown> | null
  bodyText: string | null
  scalarProperties: Record<string, unknown> | null
}

export interface MessageEvent {
  type: 'Enqueued' | 'Completed' | 'DeadLettered' | 'Abandoned'
  namespace: string
  entity: string
  messageId: string
  sequenceNumber: number
  contentType: string | null
  bodyPreview: string | null
  scalarProperties: Record<string, unknown> | null
  timestamp: string
}

// Entity tree grouping
export interface EntityGroup {
  prefix: string
  topics: TopicInfo[]
  collapsed: boolean
}
```

- [ ] **Step 2: Create API client**

```typescript
// src/AlmostServiceBus.Dashboard/src/api/client.ts
import type { NamespaceInfo, EntityOverview, MessageInfo } from '../types'

const BASE = '/api/dashboard'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

async function del(path: string): Promise<void> {
  const res = await fetch(`${BASE}${path}`, { method: 'DELETE' })
  if (!res.ok) throw new Error(`API error: ${res.status}`)
}

export const api = {
  getNamespaces: () => get<NamespaceInfo[]>('/namespaces'),

  getEntities: (ns: string) => get<EntityOverview>(`/namespaces/${ns}/entities`),

  getMessages: (ns: string, queueName: string) =>
    get<MessageInfo[]>(`/namespaces/${ns}/queues/${queueName}/messages`),

  getDeadLetterMessages: (ns: string, queueName: string) =>
    get<MessageInfo[]>(`/namespaces/${ns}/queues/${queueName}/deadletter`),

  purgeQueue: (ns: string, queueName: string) =>
    del(`/namespaces/${ns}/queues/${queueName}/messages`),

  purgeDeadLetter: (ns: string, queueName: string) =>
    del(`/namespaces/${ns}/queues/${queueName}/deadletter`),
}
```

- [ ] **Step 3: Create SSE client**

```typescript
// src/AlmostServiceBus.Dashboard/src/api/sse.ts
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
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: add TypeScript types, API client, and SSE client"
```

---

### Task 3: App shell — three-panel layout

**Files:**
- Modify: `src/AlmostServiceBus.Dashboard/src/App.vue`
- Create: `src/AlmostServiceBus.Dashboard/src/style.css`

- [ ] **Step 1: Create global styles (dark theme)**

```css
/* src/AlmostServiceBus.Dashboard/src/style.css */
:root {
  --bg-base: #1e1e2e;
  --bg-mantle: #181825;
  --bg-crust: #11111b;
  --bg-surface: #313244;
  --text: #cdd6f4;
  --text-dim: #a6adc8;
  --text-muted: #6c7086;
  --blue: #89b4fa;
  --green: #a6e3a1;
  --red: #f38ba8;
  --yellow: #f9e2af;
  --mauve: #cba6f7;
  --peach: #fab387;
  --border: #45475a;
  --border-subtle: #313244;
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg-base);
  color: var(--text);
  font-size: 13px;
}

button {
  cursor: pointer;
  border: none;
  border-radius: 4px;
  font-size: 11px;
  padding: 4px 10px;
}
```

- [ ] **Step 2: Create App shell**

```vue
<!-- src/AlmostServiceBus.Dashboard/src/App.vue -->
<script setup lang="ts">
import { ref } from 'vue'
import type { MessageInfo } from './types'
import EntityTree from './components/EntityTree.vue'
import MessageList from './components/MessageList.vue'
import MessageDetail from './components/MessageDetail.vue'

const selectedNamespace = ref('default')
const selectedEntity = ref<string | null>(null)
const selectedEntityType = ref<'queue' | 'topic' | null>(null)
const selectedMessage = ref<MessageInfo | null>(null)
</script>

<template>
  <div class="app">
    <EntityTree
      v-model:namespace="selectedNamespace"
      v-model:entity="selectedEntity"
      v-model:entityType="selectedEntityType"
      @select="selectedMessage = null"
    />
    <MessageList
      v-if="selectedEntity"
      :namespace="selectedNamespace"
      :entity="selectedEntity"
      :entity-type="selectedEntityType"
      v-model:selectedMessage="selectedMessage"
    />
    <MessageDetail
      v-if="selectedMessage"
      :message="selectedMessage"
    />
    <div v-if="!selectedEntity" class="empty-state">
      <p>Select an entity from the sidebar to browse messages</p>
    </div>
  </div>
</template>

<style scoped>
.app {
  display: flex;
  height: 100vh;
  overflow: hidden;
}
.empty-state {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-muted);
}
</style>
```

- [ ] **Step 3: Create stub components**

Create minimal stub components for `EntityTree.vue`, `MessageList.vue`, `MessageDetail.vue` that just render a placeholder div, so the app compiles and renders the shell.

- [ ] **Step 4: Verify app renders**

Run: `npm run dev`

Expected: Three-panel shell renders with sidebar placeholder and empty state.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add three-panel app shell with dark theme"
```

---

### Task 4: EntityTree component (left panel)

**Files:**
- Create: `src/AlmostServiceBus.Dashboard/src/components/EntityTree.vue`
- Create: `src/AlmostServiceBus.Dashboard/src/components/NamespaceTabs.vue`
- Create: `src/AlmostServiceBus.Dashboard/src/composables/useEntities.ts`

- [ ] **Step 1: Create useEntities composable**

Handles fetching entities, polling for updates, and grouping topics by common namespace prefix.

```typescript
// src/AlmostServiceBus.Dashboard/src/composables/useEntities.ts
import { ref, computed, watch } from 'vue'
import type { EntityOverview, QueueInfo, TopicInfo, EntityGroup, NamespaceInfo } from '../types'
import { api } from '../api/client'

export function useEntities(selectedNamespace: () => string) {
  const namespaces = ref<NamespaceInfo[]>([])
  const entities = ref<EntityOverview | null>(null)
  const filter = ref('')

  async function refreshNamespaces() {
    try { namespaces.value = await api.getNamespaces() } catch {}
  }

  async function refreshEntities() {
    try { entities.value = await api.getEntities(selectedNamespace()) } catch {}
  }

  // Poll every 3 seconds
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

  // Group topics by namespace prefix (part before '/')
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
      collapsed: false,
    }))
  })

  const filteredQueues = computed<QueueInfo[]>(() => {
    if (!entities.value) return []
    const f = filter.value.toLowerCase()
    return f ? entities.value.queues.filter(q => q.name.toLowerCase().includes(f)) : entities.value.queues
  })

  return { namespaces, entities, filter, topicGroups, filteredQueues, startPolling, stopPolling, refreshEntities }
}
```

- [ ] **Step 2: Create NamespaceTabs component**

```vue
<!-- src/AlmostServiceBus.Dashboard/src/components/NamespaceTabs.vue -->
<script setup lang="ts">
import type { NamespaceInfo } from '../types'

defineProps<{ namespaces: NamespaceInfo[]; selected: string }>()
const emit = defineEmits<{ 'update:selected': [value: string] }>()
</script>

<template>
  <div class="tabs">
    <div
      v-for="ns in namespaces"
      :key="ns.name"
      class="tab"
      :class="{ active: ns.name === selected }"
      @click="emit('update:selected', ns.name)"
    >
      {{ ns.name }}
    </div>
  </div>
</template>

<style scoped>
.tabs { display: flex; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.tab { padding: 8px 14px; font-size: 11px; color: var(--text-muted); cursor: pointer; }
.tab.active { background: var(--bg-base); border-bottom: 2px solid var(--blue); color: var(--blue); font-weight: 600; }
</style>
```

- [ ] **Step 3: Create EntityTree component**

The full left panel: namespace tabs, search filter, grouped topic tree with queues section.

```vue
<!-- src/AlmostServiceBus.Dashboard/src/components/EntityTree.vue -->
<script setup lang="ts">
import { onMounted, onUnmounted, toRef } from 'vue'
import NamespaceTabs from './NamespaceTabs.vue'
import { useEntities } from '../composables/useEntities'

const ns = defineModel<string>('namespace', { required: true })
const entity = defineModel<string | null>('entity', { required: true })
const entityType = defineModel<'queue' | 'topic' | null>('entityType', { required: true })
const emit = defineEmits<{ select: [] }>()

const { namespaces, filter, topicGroups, filteredQueues, startPolling, stopPolling } =
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
      <input v-model="filter" placeholder="🔍 Filter entities..." />
    </div>

    <div class="tree">
      <div class="section-header">Queues</div>
      <div
        v-for="q in filteredQueues" :key="q.name"
        class="entity-row" :class="{ selected: entity === q.name }"
        @click="selectEntity(q.name, 'queue')"
      >
        📥 {{ q.name }}
        <span v-if="q.deadLetterCount > 0" class="badge-red">{{ q.deadLetterCount }}</span>
      </div>

      <div class="section-header">Topics</div>
      <template v-for="group in topicGroups" :key="group.prefix">
        <div class="group-header" @click="group.collapsed = !group.collapsed">
          <span class="chevron">{{ group.collapsed ? '▸' : '▾' }}</span>
          {{ group.prefix }}
        </div>
        <template v-if="!group.collapsed">
          <div
            v-for="t in group.topics" :key="t.name"
            class="entity-row indent" :class="{ selected: entity === t.name }"
            @click="selectEntity(t.name, 'topic')"
          >
            📡 {{ shortName(t.name) }}
          </div>
          <div
            v-for="t in group.topics"
            :key="t.name + '-subs'"
          >
            <div
              v-for="s in t.subscriptions" :key="s.name"
              class="sub-row"
            >
              ↳ {{ s.name }}
            </div>
          </div>
        </template>
      </template>
    </div>

    <div class="footer">
      {{ filteredQueues.length }} queues ·
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
.group-header { padding: 4px 4px 4px 12px; color: var(--text-dim); cursor: pointer; font-size: 11px; }
.chevron { color: var(--text-muted); margin-right: 4px; }
.entity-row { padding: 4px 4px 4px 12px; cursor: pointer; border-radius: 4px; font-size: 11px; }
.entity-row.indent { padding-left: 28px; }
.entity-row:hover { background: var(--bg-surface); }
.entity-row.selected { background: var(--bg-surface); color: var(--blue); }
.sub-row { padding: 3px 4px 3px 40px; color: var(--text-dim); font-size: 10px; }
.badge-red { background: var(--red); color: var(--bg-crust); border-radius: 8px; padding: 0 6px; font-size: 10px; margin-left: 4px; }
.footer { border-top: 1px solid var(--border); padding: 8px 12px; background: var(--bg-mantle); font-size: 10px; color: var(--text-muted); }
</style>
```

- [ ] **Step 4: Verify entity tree renders with mock or live data**

Run: `npm run dev`

Expected: Left panel shows with namespace tabs, search, and entity tree (empty until backend is running).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: add EntityTree with namespace tabs and topic grouping"
```

---

### Task 5: MessageList component (middle panel)

**Files:**
- Create: `src/AlmostServiceBus.Dashboard/src/components/MessageList.vue`
- Create: `src/AlmostServiceBus.Dashboard/src/components/MessageRow.vue`
- Create: `src/AlmostServiceBus.Dashboard/src/composables/useMessages.ts`

- [ ] **Step 1: Create useMessages composable**

Fetches messages for the selected entity and integrates SSE for real-time updates.

```typescript
// src/AlmostServiceBus.Dashboard/src/composables/useMessages.ts
import { ref, watch, onUnmounted } from 'vue'
import type { MessageInfo, MessageEvent } from '../types'
import { api } from '../api/client'
import { connectSse } from '../api/sse'

export function useMessages(ns: () => string, entity: () => string | null) {
  const messages = ref<MessageInfo[]>([])
  const connected = ref(false)
  let source: EventSource | null = null

  async function refresh() {
    const e = entity()
    if (!e) { messages.value = []; return }
    try {
      messages.value = await api.getMessages(ns(), e)
    } catch {}
  }

  function startSse() {
    stopSse()
    const e = entity()
    if (!e) return

    source = connectSse(ns(), e, (evt: MessageEvent) => {
      if (evt.type === 'Enqueued') {
        // Add to top of list
        messages.value.unshift({
          messageId: evt.messageId,
          sequenceNumber: evt.sequenceNumber,
          contentType: evt.contentType,
          correlationId: null,
          deliveryCount: 0,
          enqueuedTimeUtc: evt.timestamp,
          subject: null,
          applicationProperties: null,
          bodyText: evt.bodyPreview,
          scalarProperties: evt.scalarProperties,
        })
        // Keep list bounded
        if (messages.value.length > 100) messages.value.pop()
      }
    })

    source.onopen = () => { connected.value = true }
    source.onerror = () => { connected.value = false }
  }

  function stopSse() {
    source?.close()
    source = null
    connected.value = false
  }

  return { messages, connected, refresh, startSse, stopSse }
}
```

- [ ] **Step 2: Create MessageRow component**

```vue
<!-- src/AlmostServiceBus.Dashboard/src/components/MessageRow.vue -->
<script setup lang="ts">
import type { MessageInfo } from '../types'

defineProps<{ message: MessageInfo; selected: boolean }>()
defineEmits<{ select: [] }>()

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  return `${Math.floor(diff / 3600000)}h ago`
}
</script>

<template>
  <div class="row" :class="{ selected }" @click="$emit('select')">
    <div class="header">
      <span class="msg-id">{{ message.messageId.substring(0, 12) }}</span>
      <span class="time">{{ timeAgo(message.enqueuedTimeUtc) }}</span>
    </div>
    <div v-if="message.scalarProperties" class="tags">
      <span
        v-for="(val, key) in message.scalarProperties" :key="key"
        class="tag"
      >
        {{ key }}: {{ JSON.stringify(val) }}
      </span>
    </div>
  </div>
</template>

<style scoped>
.row { padding: 8px 12px; border-bottom: 1px solid var(--border-subtle); cursor: pointer; }
.row:hover { background: var(--bg-surface); }
.row.selected { background: var(--bg-surface); border-left: 3px solid var(--blue); }
.header { display: flex; justify-content: space-between; align-items: center; }
.msg-id { color: var(--text); font-weight: 500; font-size: 11px; }
.time { color: var(--text-muted); font-size: 9px; }
.tags { display: flex; gap: 4px; margin-top: 5px; flex-wrap: wrap; }
.tag { background: var(--bg-crust); color: var(--green); padding: 1px 6px; border-radius: 3px; font-size: 9px; font-family: monospace; }
.tag:nth-child(even) { color: var(--yellow); }
</style>
```

- [ ] **Step 3: Create MessageList component**

```vue
<!-- src/AlmostServiceBus.Dashboard/src/components/MessageList.vue -->
<script setup lang="ts">
import { watch, onMounted, onUnmounted } from 'vue'
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
  useMessages(() => props.namespace, () => props.entity)

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
        <button class="btn-danger" @click="/* purge */">Purge</button>
      </div>
      <div v-if="parentPath(entity)" class="parent">{{ parentPath(entity) }}</div>
    </div>

    <div class="tabs">
      <div class="tab active">Messages</div>
      <div class="tab">Dead Letter</div>
      <div class="tab">Properties</div>
    </div>

    <div class="live-indicator" :class="{ connected }">
      {{ connected ? '● Live' : '○ Disconnected' }}
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
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: add MessageList with SSE integration and scalar tags"
```

---

### Task 6: MessageDetail component (right panel)

**Files:**
- Create: `src/AlmostServiceBus.Dashboard/src/components/MessageDetail.vue`
- Create: `src/AlmostServiceBus.Dashboard/src/components/JsonViewer.vue`

- [ ] **Step 1: Create JsonViewer component**

Simple syntax-highlighted JSON display.

```vue
<!-- src/AlmostServiceBus.Dashboard/src/components/JsonViewer.vue -->
<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ json: string }>()

const formatted = computed(() => {
  try {
    return JSON.stringify(JSON.parse(props.json), null, 2)
  } catch {
    return props.json
  }
})
</script>

<template>
  <pre class="json-viewer">{{ formatted }}</pre>
</template>

<style scoped>
.json-viewer {
  margin: 0;
  font-family: 'Cascadia Code', 'Fira Code', monospace;
  font-size: 11px;
  line-height: 1.6;
  color: var(--text);
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
```

- [ ] **Step 2: Create MessageDetail component**

```vue
<!-- src/AlmostServiceBus.Dashboard/src/components/MessageDetail.vue -->
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
        <span class="value">{{ message.contentType ?? '—' }}</span>
      </div>
      <div class="meta-row">
        <span class="label">Correlation ID</span>
        <span class="value mono">{{ message.correlationId ?? '—' }}</span>
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
.tab-content { flex: 1; overflow-y: auto; }
.body-viewer { padding: 12px 16px; background: var(--bg-crust); height: 100%; }
.props-viewer { padding: 12px 16px; }
.prop-row { display: grid; grid-template-columns: 200px 1fr; gap: 4px 12px; padding: 4px 0; border-bottom: 1px solid var(--border-subtle); font-size: 11px; }
.prop-key { color: var(--blue); font-family: monospace; }
.prop-val { color: var(--text); font-family: monospace; word-break: break-all; }
.empty { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: add MessageDetail panel with JSON viewer and property tabs"
```

---

### Task 7: Vite.AspNetCore integration

**Files:**
- Modify: `src/AlmostServiceBus.Host/AlmostServiceBus.Host.csproj`
- Modify: `src/AlmostServiceBus.Host/Program.cs`

Wire the Vue app into the Kestrel host so it's served from the same origin.

- [ ] **Step 1: Add Vite.AspNetCore NuGet package**

```bash
cd src/AlmostServiceBus.Host
dotnet add package Vite.AspNetCore
```

- [ ] **Step 2: Update Program.cs to serve the Vue app**

```csharp
// Add to Program.cs, after building the app:

if (app.Environment.IsDevelopment())
{
    // In development, proxy to Vite dev server for HMR
    app.UseViteDevelopmentServer();
}

app.UseStaticFiles(); // Serve built Vue assets from wwwroot/

// Add a fallback route for the SPA — serves index.html for unknown routes
// (must be after API routes)
app.MapFallbackToFile("index.html");
```

Add to `appsettings.Development.json`:
```json
{
  "Vite": {
    "PackageDirectory": "../AlmostServiceBus.Dashboard",
    "Server": {
      "AutoRun": true,
      "Port": 5173
    }
  }
}
```

- [ ] **Step 3: Build Vue app for production**

```bash
cd src/AlmostServiceBus.Dashboard
npm run build
```

Expected: Built files appear in `src/AlmostServiceBus.Host/wwwroot/`.

- [ ] **Step 4: Verify dashboard loads from emulator**

Start the emulator: `dotnet run --project src/AlmostServiceBus.Host`

Open `http://localhost:{HTTP_PORT}` — should show the dashboard.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: integrate Vue dashboard with Vite.AspNetCore"
```
