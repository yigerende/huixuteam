<script setup lang="ts">
import {onUnmounted,ref,watch} from 'vue';
import {api} from '../lib/api';
type PersonalPlan={status:string;label:string;fields?:{name:string;value:string}[]};
type Item={stage:string;attempt:number;status:string;started_at:string|null;finished_at:string|null;duration_seconds:number|null;reason:string;personal_plan?:PersonalPlan};
type Recovery={pending:boolean;running:boolean;alert:boolean;label:string;reason:string;started_at:string|null;next_at:string|null;retries:number;checks:number;cleanups:number;switches:number;fallbacks:number};
type Timeline={recovery?:Recovery;items:Item[];active:boolean;generated_at:string};
const props=defineProps<{id:string;status:string}>();
const data=ref<Timeline>(),error=ref(''),loading=ref(false);
let controller:AbortController|undefined,timer:ReturnType<typeof setTimeout>|undefined,sequence=0,disposed=false;
function timestamp(value:string|null){return value?new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(new Date(value)):'未记录'}
function duration(n:number|null){if(n===null)return '未记录';if(n<1)return '不足 1 秒';if(n<60)return `${n} 秒`;return `${Math.floor(n/60)} 分 ${n%60} 秒`}
function tone(state:string){if(['已完成','已确认','已受理'].includes(state))return 'done';if(['进行中','排队中'].includes(state))return 'running';if(['待跟进','待核实','未受理'].includes(state))return 'attention';return 'muted'}
function planDetails(plan:PersonalPlan){return plan.fields?.map(field=>`${field.name}：${field.value}`).join('\n')||plan.label}
async function load(background=false){
 clearTimeout(timer);if(disposed)return;
 if(background&&document.hidden){timer=setTimeout(()=>load(true),10000);return}
 controller?.abort();controller=new AbortController();const current=++sequence;
 loading.value=true;error.value='';
 try{const result=await api<Timeline>(`/api/admin/refunds/${encodeURIComponent(props.id)}/timeline`,{signal:controller.signal});if(current===sequence)data.value=result}
 catch(e){if(current===sequence&&(e as Error).name!=='AbortError')error.value='读取失败，请重新读取记录'}
 finally{if(current===sequence&&!disposed){loading.value=false;if(data.value?.active)timer=setTimeout(()=>load(true),10000)}}
}
watch(()=>[props.id,props.status],()=>{data.value=undefined;load()},{immediate:true});
onUnmounted(()=>{disposed=true;sequence++;controller?.abort();clearTimeout(timer)});
</script>
<template>
 <section class="stage-monitor" aria-label="办理流程记录" :aria-busy="loading">
  <header><div><h4>办理流程记录</h4><p>北京时间 · {{data?.active?'每 10 秒读取记录':'当前记录'}} · 受理不代表退款到账</p></div><button class="text-button" :disabled="loading" @click="load()">{{loading?'读取中…':'刷新流程记录'}}</button></header>
  <p v-if="error" class="form-error" role="alert">{{error}}</p>
  <div v-if="data?.recovery?.started_at" class="recovery-status" :class="{alert:data.recovery.alert}">
   <strong>{{data.recovery.label}}{{data.recovery.running?' · 正在执行':''}}</strong>
   <span v-if="data.recovery.alert">异常占用已超过告警阈值，请跟进</span>
   <p>{{data.recovery.reason}}</p>
   <p>首次异常 {{timestamp(data.recovery.started_at)}}<br/>{{data.recovery.next_at?'下次执行 '+timestamp(data.recovery.next_at):data.recovery.pending?'当前正在核对':'没有待执行的自动任务'}}</p>
   <small>已重试 {{data.recovery.retries}} 次 · 已核实 {{data.recovery.checks}} 轮 · 已清理核对 {{data.recovery.cleanups}} 轮 · 已兜底 {{data.recovery.fallbacks||0}} 轮<span v-if="data.recovery.switches"> · 已安排换号 {{data.recovery.switches}} 次</span></small>
  </div>
  <ol v-if="data" class="stage-records">
   <li v-for="(row,index) in data.items" :key="index" :class="tone(row.status)">
    <span class="stage-mark">{{row.stage}}</span>
    <div class="stage-copy"><div class="stage-top"><strong>{{row.status}}</strong><small v-if="row.attempt>1">第 {{row.attempt}} 次</small><span v-if="row.stage==='登'&&row.personal_plan" class="personal-plan" :class="{observed:row.personal_plan.status==='observed'}" :title="planDetails(row.personal_plan)" tabindex="0">{{row.personal_plan.label}}</span><time>{{timestamp(row.finished_at||row.started_at)}}</time></div>
     <div class="stage-meta"><span>耗时 {{duration(row.duration_seconds)}}</span><span v-if="row.started_at&&row.finished_at">开始 {{timestamp(row.started_at)}}</span><span v-if="row.reason">{{row.reason}}</span></div>
    </div>
   </li>
  </ol>
  <p class="stage-footnote">缺少历史记录时显示“未记录”；查看与刷新只读取已有记录。</p>
 </section>
</template>
<style scoped>
.personal-plan{font-size:11px;line-height:1.6;padding:1px 7px;border-radius:5px;background:#eeece2;color:#827353;max-width:100%;overflow-wrap:anywhere}.personal-plan.observed{background:#e1eddf;color:#47714b}.personal-plan:focus-visible{outline:2px solid #47714b;outline-offset:2px}
.recovery-status{padding:12px;margin-top:12px;border:1px solid #d5dfd0;border-radius:7px;font-size:12px;line-height:1.8}.recovery-status.alert{border-color:#d66851;background:#fff0ea}.recovery-status>span{display:block;color:#af442b}.recovery-status p{margin:5px 0}

.stage-monitor{padding:18px;background:#f7f7ef;border:1px solid var(--line);border-radius:10px}.stage-monitor header{display:flex;justify-content:space-between;align-items:start;gap:12px}.stage-monitor h4{margin:0;font-size:14px}.stage-monitor header p,.stage-footnote{font-size:11px;color:#7c856f;line-height:1.6;margin:7px 0}.stage-monitor header button{font-size:11px;white-space:nowrap}.stage-records{list-style:none;padding:0;margin:16px 0 0}.stage-records li{display:flex;gap:12px;position:relative;padding-bottom:17px}.stage-records li:not(:last-child):before{content:'';position:absolute;left:15px;top:32px;bottom:0;border-left:1px solid #dfe4d6}.stage-mark{display:grid;place-items:center;flex-shrink:0;width:32px;height:32px;border-radius:9px;font-size:16px;font-weight:650;background:#e9ebe2;color:#8a9380}.done .stage-mark{background:#e1eddf;color:#47714b}.running .stage-mark{background:#ddebf2;color:#3d6c86}.attention .stage-mark{background:#fae6cc;color:#a96620}.stage-copy{min-width:0;flex:1}.stage-top{display:flex;align-items:center;gap:9px;flex-wrap:wrap;min-height:25px}.stage-top strong{font-size:12px}.stage-top small{font-size:10px;color:#8e7757}.stage-top time{margin-left:auto;color:#78826a;font-size:11px;font-variant-numeric:tabular-nums}.stage-meta{display:flex;flex-wrap:wrap;gap:6px 14px;color:#8a927e;font-size:10px;line-height:1.7}.stage-footnote{margin-bottom:0}.muted .stage-top strong{color:#909886}@media(max-width:600px){.stage-monitor{padding:13px}.stage-top time{width:100%;margin-left:0}.stage-monitor header{flex-wrap:wrap}.stage-records li{gap:10px}}
</style>
