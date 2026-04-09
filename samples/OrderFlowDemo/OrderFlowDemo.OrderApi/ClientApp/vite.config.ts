import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5174,
    proxy: {
      '/api': {
        target: 'http://localhost:5200',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: '../wwwroot',
    emptyOutDir: true,
  },
})
