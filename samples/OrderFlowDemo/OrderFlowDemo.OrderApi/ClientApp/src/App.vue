<script setup lang="ts">
import { useSse } from './composables/useSse'
import { useDashboardStore } from './composables/useDashboardStore'
import SidebarNav from './components/SidebarNav.vue'
import ScenarioBar from './components/ScenarioBar.vue'
import MetricCards from './components/MetricCards.vue'
import PipelineFlow from './components/PipelineFlow.vue'
import ThroughputChart from './components/ThroughputChart.vue'
import SagaDonut from './components/SagaDonut.vue'
import QueueDepths from './components/QueueDepths.vue'
import WarehouseFifo from './components/WarehouseFifo.vue'
import LiveFeed from './components/LiveFeed.vue'

const { connected, onEvent } = useSse('/api/dashboard/events')
const { state, processEvent, inFlight, throughputPerSecond } = useDashboardStore()

onEvent(processEvent)
</script>

<template>
  <div class="app-shell">
    <!-- Left: charcoal sidebar -->
    <SidebarNav :connected="connected" />

    <!-- Right: top bar + scrollable content -->
    <div class="app-right">
      <header class="top-bar">
        <div class="top-bar-left">
          <h1 class="page-title">Dashboard</h1>
          <span class="sse-badge" :class="{ live: connected }">
            {{ connected ? '● SSE LIVE' : '○ DISCONNECTED' }}
          </span>
        </div>
        <ScenarioBar />
      </header>

      <main class="main-content">
        <!-- Metrics row -->
        <MetricCards
          :total="state.totalOrders"
          :completed="state.completedOrders"
          :failed="state.failedOrders"
          :in-flight="inFlight"
          :throughput="throughputPerSecond"
        />

        <!-- 2:1 grid: Pipeline + Sagas -->
        <div class="grid-2-1">
          <div class="card">
            <div class="card-title">Order Pipeline <span class="card-badge">LIVE</span></div>
            <PipelineFlow :counts="state.pipelineCounts" />
          </div>
          <div class="card">
            <div class="card-title">Saga States</div>
            <SagaDonut :counts="state.pipelineCounts" :in-flight="inFlight" />
          </div>
        </div>

        <!-- Throughput chart full width -->
        <div class="card">
          <div class="card-title">Throughput — 60 s window <span class="card-badge">STREAMING</span></div>
          <ThroughputChart :history="state.throughputHistory" />
        </div>

        <!-- Bottom 3-col grid -->
        <div class="grid-3">
          <div class="card">
            <div class="card-title">Queue Depths</div>
            <QueueDepths :counts="state.pipelineCounts" />
          </div>
          <div class="card">
            <div class="card-title">Warehouse FIFO Lanes</div>
            <WarehouseFifo :depths="state.warehouseDepths" />
          </div>
          <div class="card">
            <div class="card-title">Live Event Feed <span class="card-badge">STREAMING</span></div>
            <LiveFeed :items="state.feedItems" />
          </div>
        </div>
      </main>
    </div>
  </div>
</template>
