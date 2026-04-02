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
      @selectQueue="(q) => { selectedEntity = q; selectedEntityType = 'queue'; selectedMessage = null }"
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
