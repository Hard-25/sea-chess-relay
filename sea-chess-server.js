#!/usr/bin/env node
// Sea Chess Server — zero dependencies
// node sea-chess-server.js [port]

const http   = require('http');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
// PORT: hosting platforms (Render, Fly.io, Railway, ...) assign the port via
// the PORT env var — that always wins. process.argv[2] still works for
// running it yourself locally (`node sea-chess-server.js 3000`).
const PORT   = parseInt(process.env.PORT) || parseInt(process.argv[2]) || 3000;

class WS {
  constructor(sock){
    this.sock=sock; this._ev={}; this._buf=Buffer.alloc(0);
    sock.on('data',d=>this._data(d));
    sock.on('close',()=>this._emit('close'));
    sock.on('error',()=>this._emit('close'));
  }
  on(e,fn){(this._ev[e]=this._ev[e]||[]).push(fn);}
  _emit(e,...a){(this._ev[e]||[]).forEach(f=>f(...a));}
  _data(chunk){
    this._buf=Buffer.concat([this._buf,chunk]);
    for(;;){
      if(this._buf.length<2)return;
      const b0=this._buf[0],b1=this._buf[1];
      const masked=!!(b1&0x80);
      let plen=b1&0x7f,off=2;
      if(plen===126){if(this._buf.length<4)return;plen=this._buf.readUInt16BE(2);off=4;}
      else if(plen===127){if(this._buf.length<10)return;plen=Number(this._buf.readBigUInt64BE(2));off=10;}
      if(this._buf.length<off+(masked?4:0)+plen)return;
      let pay;
      if(masked){const m=this._buf.slice(off,off+4);off+=4;pay=Buffer.alloc(plen);for(let i=0;i<plen;i++)pay[i]=this._buf[off+i]^m[i%4];}
      else pay=this._buf.slice(off,off+plen);
      this._buf=this._buf.slice(off+plen);
      const op=b0&0x0f;
      if(op===1)this._emit('message',pay.toString('utf8'));
      else if(op===8){this.sock.destroy();return;}
      else if(op===9)this._frame(10,pay);
    }
  }
  _frame(op,pay){
    const p=Buffer.isBuffer(pay)?pay:Buffer.from(pay,'utf8');
    const l=p.length;
    let h;
    if(l<126)h=Buffer.from([0x80|op,l]);
    else if(l<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(l,2);}
    else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(l),2);}
    try{this.sock.write(Buffer.concat([h,p]));}catch{}
  }
  send(d){this._frame(1,d);}
  close(){try{this._frame(8,'');this.sock.end();}catch{}}
}

function shake(req,sock){
  const acc=crypto.createHash('sha1').update(req.headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+acc+'\r\n\r\n');
  return new WS(sock);
}

// rooms[id] = { clients: Map<uid,{ws,name,slot}>, hostId, state, numPlayers }
const rooms={};
let uid=0;

function getRoom(id){
  if(!rooms[id])rooms[id]={id,clients:new Map(),hostId:null,state:null,numPlayers:2};
  return rooms[id];
}
function wsend(ws,o){try{ws.send(JSON.stringify(o));}catch{}}
function bcast(rm,o,skip=null){const s=JSON.stringify(o);rm.clients.forEach((c,id)=>{if(id!==skip)try{c.ws.send(s);}catch{}});}
function ball(rm,o){bcast(rm,o,null);}

function assignSlots(rm){
  let s=0;
  rm.clients.forEach(c=>{c.slot=s<rm.numPlayers?s++:-1;});
}
function slotsInfo(rm){
  const out=[];
  rm.clients.forEach((c,id)=>out.push({id,name:c.name,slot:c.slot,isHost:rm.hostId===id}));
  return out;
}
function notifySlots(rm){
  rm.clients.forEach((c,id)=>wsend(c.ws,{type:'your_slot',slot:c.slot,slots:slotsInfo(rm)}));
}
function promoteHost(rm){
  for(const[id,c]of rm.clients){
    rm.hostId=id;
    wsend(c.ws,{type:'you_are_host'});
    bcast(rm,{type:'slots_update',slots:slotsInfo(rm)},id);
    console.log(`[${rm.id}] Host→${id} slot=${c.slot}`);
    return;
  }
}
function leave(rm,id){
  if(!rm.clients.has(id))return;
  const leaving=rm.clients.get(id);
  rm.clients.delete(id);
  if(rm.clients.size===0){delete rooms[rm.id];console.log(`[${rm.id}] closed`);return;}
  assignSlots(rm);
  if(rm.hostId===id)promoteHost(rm);
  ball(rm,{type:'player_left',leftSlot:leaving.slot,slots:slotsInfo(rm)});
  notifySlots(rm);
}

const server=http.createServer((req,res)=>{
  // Plain CORS — this server now lives at a public https/wss address, so
  // every client (host and guests alike) reaches it as a normal public
  // request. Nobody crosses a public→local-network boundary anymore, which
  // is what used to trip Chrome's Local Network Access permission on
  // Android (the old LAN-IP setup this replaced).
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','*');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if(req.url==='/'||req.url==='/index.html'){
    try{res.writeHead(200,{'Content-Type':'text/html;charset=utf-8'});res.end(fs.readFileSync(path.join(__dirname,'index.html')));}
    catch{res.writeHead(500);res.end('index.html not found next to server.js');}
    return;
  }

  // API: active rooms list
  if(req.url==='/rooms'){
    const list=Object.values(rooms).map(rm=>({
      id:rm.id,
      players:rm.clients.size,
      numSlots:rm.numPlayers,
      phase:rm.state?.phase||'waiting'
    }));
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify({ok:true,rooms:list}));
    return;
  }

  // API: server status
  if(req.url==='/status'){
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify({ok:true,rooms:Object.keys(rooms).length}));
    return;
  }

  res.writeHead(404);res.end();
});

server.on('upgrade',(req,sock)=>{
  if(req.url!=='/ws'){sock.destroy();return;}
  const ws=shake(req,sock);
  const myId=++uid;
  let rm=null;

  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw);}catch{return;}

    if(m.type==='join'){
      const rid=(m.room||'default').replace(/[^a-zA-Z0-9_-]/g,'_').slice(0,30);
      rm=getRoom(rid);
      rm.clients.set(myId,{ws,name:(m.name||`P${myId}`).slice(0,20),slot:-1});
      if(!rm.hostId)rm.hostId=myId;
      assignSlots(rm);
      const me=rm.clients.get(myId);
      wsend(ws,{type:'joined',id:myId,slot:me.slot,isHost:rm.hostId===myId,roomId:rid,slots:slotsInfo(rm)});
      if(rm.state)wsend(ws,{type:'state',state:rm.state});
      bcast(rm,{type:'player_joined',slots:slotsInfo(rm)},myId);
      notifySlots(rm);
      console.log(`[${rid}] #${myId} slot=${me.slot} (${rm.clients.size} total)`);
      return;
    }

    if(!rm)return;

    // HOST pushes state → relay to all others
    if(m.type==='state'){
      if(rm.hostId!==myId)return;
      rm.state=m.state;
      if(m.state?.numPlayers){
        rm.numPlayers=m.state.numPlayers;
        assignSlots(rm);
        notifySlots(rm);
      }
      bcast(rm,{type:'state',state:m.state},myId);
      return;
    }

    // GUEST action → forward to host only
    if(m.type==='action'){
      if(rm.hostId===myId)return; // host doesn't send actions to itself
      const h=rm.clients.get(rm.hostId);
      if(h)wsend(h.ws,{type:'action',action:m.action,fromSlot:rm.clients.get(myId)?.slot??-1,fromId:myId});
      return;
    }

    if(m.type==='chat'){
      const c=rm.clients.get(myId);
      if(!c)return;
      if(c.slot<0)return;
      const text=String(m.text||'').trim().slice(0,200);
      if(!text)return;
      // Koristi mySlot koji klijent šalje (= G.players index), fallback na c.slot
      const fromSlot = (m.mySlot !== undefined && m.mySlot >= 0) ? Number(m.mySlot) : c.slot;
      const fromName=(rm.state?.players?.[fromSlot]?.name)||c.name||`P${fromSlot}`;
      const isEmoji=!!m.isEmoji;
      // Send fromSlot in multiple fields for old/new client compatibility
      const chatMsg = (toSlot, dm) => ({
        type:'chat', fromSlot, slot:fromSlot, from:fromSlot,
        fromName, name:fromName, toSlot, text, dm, isEmoji
      });
      if(m.toSlot!=null&&m.toSlot>=0){
        rm.clients.forEach((tc,tid)=>{
          if(tc.slot===m.toSlot||tid===myId)
            wsend(tc.ws, chatMsg(m.toSlot, true));
        });
      }else{
        rm.clients.forEach(tc=>{
          if(tc.slot>=0) wsend(tc.ws, chatMsg(null, false));
        });
      }
      return;
    }

    if(m.type==='leave'){leave(rm,myId);rm=null;return;}
  });

  ws.on('close',()=>{if(rm)leave(rm,myId);});
});

server.listen(PORT,'0.0.0.0',()=>{
  console.log(`\n⚓  Sea Chess relay server listening on port ${PORT}`);
  console.log(`   Meant to run on a public host (Render/Fly.io/Railway/...) behind https/wss.`);
  console.log(`   Point the game's RELAY_URL at this server's public wss:// address.\n`);
});
