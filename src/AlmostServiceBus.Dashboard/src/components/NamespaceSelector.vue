<script setup lang="ts">
import { ref, computed } from 'vue'
import type { NamespaceInfo } from '../types'

const props = defineProps<{ namespaces: NamespaceInfo[]; selected: string }>()
const emit = defineEmits<{ 'update:selected': [value: string] }>()

const query = ref('')
const open = ref(false)

const filtered = computed(() => {
  const q = query.value.toLowerCase()
  return q
    ? props.namespaces.filter(ns => ns.name.toLowerCase().includes(q))
    : props.namespaces
})

const recents = computed(() =>
  [...props.namespaces]
    .sort((a, b) => new Date(b.lastActivityAt).getTime() - new Date(a.lastActivityAt).getTime())
    .slice(0, 5)
)

function select(name: string) {
  emit('update:selected', name)
  query.value = ''
  open.value = false
}

function onFocus() {
  open.value = true
  query.value = ''
}

function onBlur() {
  setTimeout(() => { open.value = false }, 150)
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  return `${Math.floor(diff / 3600000)}h ago`
}
</script>

<template>
  <div class="ns-selector">
    <div class="input-row">
      <input
        :value="open ? query : selected"
        :placeholder="selected || 'Select namespace...'"
        @input="query = ($event.target as HTMLInputElement).value"
        @focus="onFocus"
        @blur="onBlur"
      />
    </div>

    <!-- Recents (always visible below search) -->
    <div v-if="recents.length > 0 && !open" class="recents">
      <div
        v-for="(ns, i) in recents" :key="ns.name"
        class="recent-item"
        :class="{ active: ns.name === selected }"
        :style="{ opacity: 1 - (i * 0.15) }"
        @click="select(ns.name)"
      >
        <span class="ns-name">{{ ns.name }}</span>
        <span class="ns-meta">{{ ns.queueCount }}q · {{ ns.topicCount }}t · {{ timeAgo(ns.lastActivityAt) }}</span>
      </div>
    </div>

    <!-- Full dropdown (when searching) -->
    <div v-if="open && filtered.length > 0" class="dropdown">
      <div
        v-for="ns in filtered" :key="ns.name"
        class="dropdown-item" :class="{ active: ns.name === selected }"
        @mousedown.prevent="select(ns.name)"
      >
        <span class="ns-name">{{ ns.name }}</span>
        <span class="ns-counts">{{ ns.queueCount }}q · {{ ns.topicCount }}t</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.ns-selector { position: relative; border-bottom: 1px solid var(--dark-border); background: var(--dark); }
.input-row { display: flex; align-items: center; padding: 8px 10px; gap: 6px; }
.input-row input {
  flex: 1; background: var(--dark-surface); border: 1px solid var(--dark-border); border-radius: 6px;
  padding: 6px 10px; color: var(--dark-text); font-size: 11px; outline: none;
  min-width: 0; transition: border-color 0.15s;
}
.input-row input:focus { border-color: var(--blue); }
.input-row input::placeholder { color: var(--blue); }

.recents { padding: 2px 6px 6px; }
.recent-item {
  display: flex; justify-content: space-between; align-items: center;
  padding: 4px 8px; cursor: pointer; font-size: 10px; border-radius: 5px;
  color: var(--dark-text); transition: background 0.1s;
}
.recent-item:hover { background: var(--dark-surface); }
.recent-item.active { color: var(--blue); background: var(--dark-surface); }
.ns-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; min-width: 0; }
.ns-meta { font-size: 9px; color: var(--dark-text-muted); white-space: nowrap; margin-left: 8px; flex-shrink: 0; }

.dropdown {
  position: absolute; top: 100%; left: 0; right: 0; z-index: 10;
  background: var(--dark-surface); border: 1px solid var(--dark-border);
  border-top: none; max-height: 240px; overflow-y: auto;
  box-shadow: 0 8px 24px rgba(0,0,0,0.4);
  border-radius: 0 0 6px 6px;
}
.dropdown-item {
  display: flex; justify-content: space-between; align-items: center;
  padding: 7px 12px; cursor: pointer; font-size: 11px; color: var(--dark-text);
  transition: background 0.1s;
}
.dropdown-item:hover { background: var(--dark); }
.dropdown-item.active { color: var(--blue); }
.ns-counts { font-size: 9px; color: var(--dark-text-muted); white-space: nowrap; margin-left: 8px; }
</style>
