/**
 * 大航海期貨所 - 聯機客戶端
 * 預設：與網頁同網域連線（Render 部署時用 wss://同網域）
 * 本機 file:// 則連 ws://127.0.0.1:8766
 * 可在 localStorage.netUrl 覆寫
 */
(function (global) {
  const DEFAULT_PORT = 8766;

  function defaultUrl() {
    const saved = localStorage.getItem('netUrl');
    if (saved) return saved;
    // file:// 開啟
    if (!location.hostname || location.protocol === 'file:') {
      return `ws://127.0.0.1:${DEFAULT_PORT}`;
    }
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    // 正式站（GitHub Pages / Render 靜態）：若有設定 window.GAME_WS_URL 優先
    if (global.GAME_WS_URL) return global.GAME_WS_URL;
    // 同網域部署（Render 單一服務同時提供網頁 + WebSocket）
    // https 頁面不帶埠號，走 443 的 wss
    if (location.protocol === 'https:') {
      return `wss://${location.host}`;
    }
    // 本機 http 開發：若頁面在 8766 則同埠，否則預設 8766
    const port = location.port || String(DEFAULT_PORT);
    if (port && port !== '80') {
      return `ws://${location.hostname}:${port === '8080' || port === '5500' || port === '3000' ? DEFAULT_PORT : port}`;
    }
    return `ws://${location.hostname}:${DEFAULT_PORT}`;
  }

  const Net = {
    ws: null,
    url: defaultUrl(),
    connected: false,
    playerId: null,
    room: null,
    handlers: {},
    _queue: [],
    _retry: 0,
    _intentionalClose: false,

    on(type, fn) {
      if (!this.handlers[type]) this.handlers[type] = [];
      this.handlers[type].push(fn);
    },
    off(type, fn) {
      if (!this.handlers[type]) return;
      this.handlers[type] = this.handlers[type].filter((f) => f !== fn);
    },
    emit(type, data) {
      (this.handlers[type] || []).forEach((fn) => {
        try { fn(data); } catch (e) { console.error(e); }
      });
      (this.handlers['*'] || []).forEach((fn) => {
        try { fn(type, data); } catch (e) { console.error(e); }
      });
    },

    setUrl(url) {
      this.url = url;
      localStorage.setItem('netUrl', url);
    },

    connect() {
      if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
        return;
      }
      this._intentionalClose = false;
      try {
        this.ws = new WebSocket(this.url);
      } catch (e) {
        this.emit('error', { message: '無法建立連線：' + e.message });
        return;
      }
      this.ws.onopen = () => {
        this.connected = true;
        this._retry = 0;
        this.emit('open', {});
        while (this._queue.length) {
          this.ws.send(this._queue.shift());
        }
      };
      this.ws.onclose = () => {
        this.connected = false;
        this.emit('close', {});
        if (!this._intentionalClose) {
          const delay = Math.min(8000, 1000 * Math.pow(1.5, this._retry++));
          setTimeout(() => this.connect(), delay);
        }
      };
      this.ws.onerror = () => {
        this.emit('error', { message: '連線失敗，請確認聯機伺服器是否啟動（埠 8766）' });
      };
      this.ws.onmessage = (ev) => {
        let data;
        try { data = JSON.parse(ev.data); } catch (_) { return; }
        if (data.type === 'created' || data.type === 'joined' || data.type === 'reconnected') {
          this.playerId = data.playerId;
          this.room = data.room;
          localStorage.setItem('myPlayerId', String(data.playerId));
          if (data.room) localStorage.setItem('currentRoom', JSON.stringify(data.room));
          localStorage.setItem('onlineMode', '1');
        }
        if (data.type === 'room_update' && data.room) {
          this.room = data.room;
          localStorage.setItem('currentRoom', JSON.stringify(data.room));
        }
        if (data.type === 'game_start' && data.room) {
          this.room = data.room;
          localStorage.setItem('currentRoom', JSON.stringify(data.room));
          localStorage.setItem('onlineMode', '1');
        }
        this.emit(data.type, data);
      };
    },

    close() {
      this._intentionalClose = true;
      if (this.ws) this.ws.close();
    },

    send(obj) {
      const raw = JSON.stringify(obj);
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(raw);
      } else {
        this._queue.push(raw);
        this.connect();
      }
    },

    createRoom(playerName, maxPlayers) {
      this.send({ type: 'create', playerName, maxPlayers });
    },
    joinRoom(code, playerName) {
      this.send({ type: 'join', code: (code || '').toUpperCase(), playerName });
    },
    reconnect(code, playerId) {
      this.send({ type: 'reconnect', code: (code || '').toUpperCase(), playerId: parseInt(playerId, 10) });
    },
    startGame() {
      this.send({ type: 'start' });
    },
    listRooms() {
      this.send({ type: 'list_rooms' });
    },
    pushState(state) {
      this.send({ type: 'game_state', state });
    },
    sendEvent(event, payload) {
      this.send({ type: 'game_event', event, payload });
    },
    isOnline() {
      return localStorage.getItem('onlineMode') === '1';
    },
    clearOnline() {
      localStorage.removeItem('onlineMode');
    }
  };

  global.GameNet = Net;
})(window);
