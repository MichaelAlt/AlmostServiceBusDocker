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

const selectedInfo = computed(() =>
  props.namespaces.find(ns => ns.name === props.selected)
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
  // Delay to allow click on dropdown item
  setTimeout(() => { open.value = false }, 150)
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
      <span v-if="selectedInfo" class="counts">
        {{ selectedInfo.queueCount }}q · {{ selectedInfo.topicCount }}t
      </span>
    </div>
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
.ns-selector { position: relative; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.input-row { display: flex; align-items: center; padding: 6px 8px; gap: 6px; }
.input-row input {
  flex: 1; background: var(--bg-surface); border: none; border-radius: 4px;
  padding: 6px 10px; color: var(--text); font-size: 11px; outline: none;
  min-width: 0;
}
.input-row input::placeholder { color: var(--blue); }
.counts { font-size: 9px; color: var(--text-muted); white-space: nowrap; }
.dropdown {
  position: absolute; top: 100%; left: 0; right: 0; z-index: 10;
  background: var(--bg-surface); border: 1px solid var(--border);
  border-top: none; max-height: 240px; overflow-y: auto;
  box-shadow: 0 4px 12px rgba(0,0,0,0.3);
}
.dropdown-item {
  display: flex; justify-content: space-between; align-items: center;
  padding: 6px 12px; cursor: pointer; font-size: 11px;
}
.dropdown-item:hover { background: var(--bg-base); }
.dropdown-item.active { color: var(--blue); }
.ns-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ns-counts { font-size: 9px; color: var(--text-muted); white-space: nowrap; margin-left: 8px; }
</style>
