#!/usr/bin/env python3
"""Bounded production walkthroughs using only the public HTTP service."""
import argparse, concurrent.futures, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error
WORKFLOWS=['catalog-create','txn-write','snapshot-read','index-build','query-plan','sql-execute','txn-conflict','restart','checkpoint','backup-restore','changes','vacuum','transaction-close','statistics','request-identity']
class Session:
 def __init__(self,binary,root,restore=None):
  self.binary=str(binary);self.root=pathlib.Path(root);self.restore=restore;self.process=None;self.log=None
 def start(self):
  with socket.socket() as sock:
   sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1/lamina'
  args=[self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}']
  if self.restore:args+=['-restore',str(self.restore)];self.restore=None
  self.log=tempfile.TemporaryFile();self.process=subprocess.Popen(args,stdout=self.log,stderr=self.log)
  deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   if self.process.poll() is not None:
    self.log.seek(0);raise RuntimeError(self.log.read().decode())
   try:self.call('GET','/health');return self
   except (OSError,urllib.error.URLError):time.sleep(.03)
  raise RuntimeError('service did not become ready')
 def stop(self):
  if self.process and self.process.poll() is None:
   self.process.terminate()
   try:self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
  if self.log:self.log.close();self.log=None
 def call(self,method,path,data=None,status=200,raw=False):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:code=response.status;body=response.read()
  except urllib.error.HTTPError as e:code=e.code;body=e.read()
  if code!=status:raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
  return body if raw else json.loads(body)
 def begin(self,readonly=False):return self.call('POST','/transactions',{'read_only':readonly},201)['id']
 def put(self,tx,row,status=200,insert=False):return self.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row,'insert_only':insert},status)
 def commit(self,tx,status=200):return self.call('POST',f'/transactions/{tx}/commit',status=status)
 def commit_id(self,tx,request_id,status=200):return self.call('POST',f'/transactions/{tx}/commit',{'request_id':request_id},status)
 def request_status(self,request_id,status=200):return self.call('GET',f'/requests/{request_id}',status=status)
 def query(self,**kwargs):return self.call('POST','/query',{'relation':'spectra',**kwargs})
 def stats(self):return self.call('GET','/stats')
def require(condition,message):
 if not condition:raise RuntimeError(message)
def tuple_(n,band=None,amplitude=None):return {'sample':n,'band':band or f'b{n}','amplitude':n*10 if amplitude is None else amplitude,'valid':True}
def seed(s):
 schema={'name':'spectra','primary':'sample','columns':[{'name':'sample','type':'int'},{'name':'band','type':'text'},{'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]}
 s.call('POST','/relations',schema,201)
 tx=s.begin()
 for n in range(1,7):s.put(tx,tuple_(n))
 s.put(tx,{'sample':7,'band':'b7','amplitude':None,'valid':False})
 return s.commit(tx)['sequence']
def walkthrough(name,binary):
 with tempfile.TemporaryDirectory(prefix='lamina-flow-') as temp:
  temp=pathlib.Path(temp);s=Session(binary,temp/'source').start();other=None
  try:
   seed_seq=seed(s);observations={}
   if name=='catalog-create':
    before=s.stats()['sequence'];bad={'name':'invalid','primary':'missing','columns':[{'name':'value','type':'int'}]}
    s.call('POST','/relations',bad,400);require(s.stats()['sequence']==before,'invalid schema changed sequence')
    cat=s.call('GET','/catalog');require(len(cat['relations'])==1,'catalog mismatch');observations=cat
   elif name=='txn-write':
    tx=s.begin();s.put(tx,tuple_(8));s.put(tx,tuple_(9));require(s.query()['matched']==7,'uncommitted tuples leaked')
    result=s.commit(tx);require(result['writes']==2 and s.query()['matched']==9,'atomic commit failed')
    bad=s.begin();s.put(bad,{'sample':10,'band':'x','amplitude':'bad','valid':True},400)
    s.call('POST',f'/transactions/{bad}/rollback');observations=result
   elif name=='snapshot-read':
    old=s.begin();writer=s.begin();s.put(writer,tuple_(1,amplitude=555));s.commit(writer)
    q={'where':[{'column':'sample','op':'=','value':1}]}
    require(s.query(transaction=old,**q)['rows'][0]['amplitude']==10,'repeatable read violated')
    require(s.query(**q)['rows'][0]['amplitude']==555,'current snapshot stale')
    s.put(old,tuple_(8));require(s.query(transaction=old)['matched']==8,'read-own-write scan missing')
    require(s.query(at=seed_seq)['rows'][0]['amplitude']==10,'time travel failed')
    s.call('POST',f'/transactions/{old}/rollback');s.call('POST','/query',{'relation':'spectra','at':999},410);observations={'old':10,'current':555}
   elif name=='index-build':
    definition={'name':'by_band','relation':'spectra','column':'band','unique':True}
    s.call('POST','/indexes',definition,201)
    a=s.begin();b=s.begin();s.put(a,tuple_(8,band='shared'));s.put(b,tuple_(9,band='shared'));s.commit(a);s.commit(b,409)
    swap=s.begin();s.put(swap,tuple_(1,band='b2'));s.put(swap,tuple_(2,band='b1'));s.commit(swap)
    duplicate=s.begin();s.put(duplicate,tuple_(10,band='b3'));s.commit(duplicate,409)
    observations=s.call('GET','/catalog')
   elif name=='query-plan':
    s.call('POST','/indexes',{'name':'by_amplitude','relation':'spectra','column':'amplitude'},201)
    at=s.stats()['sequence'];tx=s.begin();s.put(tx,tuple_(1,amplitude=999));s.commit(tx)
    q={'where':[{'column':'amplitude','op':'<=','value':20}]}
    fast=s.query(**q);slow=s.query(force_scan=True,**q)
    require(fast['rows']==slow['rows'] and fast['plan']['access']=='index','index/scan mismatch')
    historical=s.query(at=at,**q);require(len(historical['rows'])==2,'historical index lost prior value')
    observations={'indexed':fast,'historical':historical}
   elif name=='sql-execute':
    sql=s.call('POST','/sql',{'sql':'SELECT sample, amplitude FROM spectra WHERE amplitude >= 30 AND valid = true ORDER BY amplitude DESC LIMIT 2;'})
    structured=s.query(columns=['sample','amplitude'],where=[{'column':'amplitude','op':'>=','value':30},{'column':'valid','op':'=','value':True}],order=[{'column':'amplitude','desc':True}],limit=2)
    require(sql['rows']==structured['rows'],'SQL and structured query diverged')
    count=s.call('POST','/sql',{'sql':'SELECT count(amplitude) FROM spectra;'})
    total=s.call('POST','/sql',{'sql':'SELECT sum(amplitude) FROM spectra;'})
    require(count['rows']==[{'count':6}] and total['rows']==[{'sum':210}],'aggregate semantics wrong')
    s.call('POST','/sql',{'sql':'SELECT * FROM spectra; SELECT * FROM spectra;'},400);observations={'projection':sql['rows'],'count':count['rows'],'sum':total['rows']}
   elif name=='txn-conflict':
    a=s.begin();b=s.begin();s.put(a,tuple_(1,amplitude=101));s.put(b,tuple_(1,amplitude=202));s.put(b,tuple_(8))
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(s.commit,a),pool.submit(s.commit,b)]
     results=[]
     for job in jobs:
      try:results.append({'commit':job.result()})
      except RuntimeError as e:
       require('got 409' in str(e),'unexpected concurrent failure');results.append({'conflict':True})
    require(sum('conflict' in r for r in results)==1,'exactly one writer must lose')
    rows=s.query()['rows'];one=next(r for r in rows if r['sample']==1)
    require((one['amplitude']==202)==any(r['sample']==8 for r in rows),'conflicting commit partially published')
    s.commit(a,404);s.commit(b,404);observations={'results':results,'rows':rows}
   elif name=='restart':
    before=s.query()['rows'];s.stop()
    with (s.root/'journal.lmw').open('ab') as f:f.write(b'\x00\x00\x00')
    s.start();require(s.query()['rows']==before,'tail recovery changed committed data');require(s.stats()['recovered_tail_bytes']==3,'tail recovery not reported')
    s.stop();broken=temp/'broken';shutil.copytree(s.root,broken)
    data=bytearray((broken/'journal.lmw').read_bytes());data[-1]^=1;(broken/'journal.lmw').write_bytes(data)
    result=subprocess.run([str(binary),'-data',str(broken),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(result.returncode!=0 and 'checksum' in result.stderr,'full frame corruption accepted')
    observations={'tail_bytes':3,'corruption':'rejected'}
   elif name=='checkpoint':
    at=s.call('POST','/checkpoint')['sequence'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    before=s.query()['rows'];s.stop();s.start();require(s.query()['rows']==before,'checkpoint plus suffix recovery failed')
    locked=subprocess.run([str(binary),'-data',str(s.root),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(locked.returncode!=0 and 'already open' in locked.stderr,'exclusive directory ownership missing');observations={'checkpoint':at,'head':s.stats()['sequence']}
   elif name=='backup-restore':
    backup=s.call('GET','/backup',raw=True);path=temp/'portable.lmi';path.write_bytes(backup)
    frozen=s.query()['rows'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    other=Session(binary,temp/'restored',path).start();require(other.query()['rows']==frozen,'backup mutated with source')
    tx=other.begin();other.put(tx,tuple_(9));result=other.commit(tx);other.stop();other.start()
    require(other.query()['matched']==8,'restored journal base does not continue')
    other.call('GET','/changes?after=0',status=410)
    broken=json.loads(backup);broken['sha256']='0'*64;bad=temp/'bad.lmi';bad.write_text(json.dumps(broken))
    failed=subprocess.run([str(binary),'-restore',str(bad),'-data',str(temp/'badrestore')],capture_output=True,text=True,timeout=5)
    require(failed.returncode!=0 and not (temp/'badrestore').exists(),'invalid backup published');observations={'restored_commit':result,'original_sequence':seed_seq}
   elif name=='changes':
    tx=s.begin()
    for n in range(8,12):s.put(tx,tuple_(n))
    committed=s.commit(tx);page=s.call('GET',f'/changes?after={seed_seq}&limit=1')
    require(len(page['records'])==1 and len(page['records'][0]['writes'])==4 and page['cursor']==committed['sequence'],'change page split transaction')
    s.call('GET','/changes?after=999',status=400);observations=page
   elif name=='vacuum':
    old=s.begin();tx=s.begin();s.put(tx,tuple_(1,amplitude=99));s.commit(tx);requested=s.stats()['sequence']
    pinned=s.call('POST','/vacuum',{'horizon':requested});require(pinned['pinned'] and pinned['horizon']==seed_seq,'vacuum crossed active snapshot')
    require(s.query(transaction=old)['rows'][0]['amplitude']==10,'active snapshot lost')
    s.call('POST',f'/transactions/{old}/rollback');reclaimed=s.call('POST','/vacuum',{'horizon':requested})
    require(reclaimed['removed']>=1,'obsolete version not reclaimed');s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    s.call('POST','/checkpoint');s.stop();s.start();require(s.query()['rows'][0]['amplitude']==99,'vacuum did not survive restart');observations={'pinned':pinned,'reclaimed':reclaimed}
   elif name=='transaction-close':
    tx=s.begin();s.put(tx,tuple_(8));s.call('POST',f'/transactions/{tx}/rollback');s.commit(tx,404)
    require(s.query()['matched']==7 and not s.stats()['active'],'rollback leaked state or pin')
    tx=s.begin(True);s.put(tx,tuple_(9),409);s.call('POST',f'/transactions/{tx}/rollback');observations={'rollback':'discarded','read_only':'enforced'}
   elif name=='statistics':
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    q=s.query(where=[{'column':'amplitude','op':'>=','value':40}],limit=2)
    require(q['matched']==3 and q['scanned']==3 and len(q['rows'])==2 and q['truncated'],'query fact meanings conflated')
    observations={'query':q,'stats':s.stats()}
   elif name=='request-identity':
    # A: acknowledgement lost -> identical retry replays the exact commit
    rid='demo-request-0001'
    first=s.begin();s.put(first,tuple_(20),insert=True)
    first_result=s.commit_id(first,rid)
    require(first_result['replayed'] is False and first_result['writes']==1,'first identity binding must publish')
    lookup=s.request_status(rid)
    require(lookup['status']=='committed' and lookup['sequence']==first_result['sequence'],'identity lookup lost the determined result')
    retry=s.begin();s.put(retry,tuple_(20))
    retry_result=s.commit_id(retry,rid)
    require(retry_result['replayed'] and retry_result['sequence']==first_result['sequence'] and retry_result['writes']==1,'identical retry did not replay the same result')
    require(s.query()['matched']==8 and s.stats()['sequence']==first_result['sequence'],'retry opened a second write path')
    # B: the same identity carrying a different logical write is rejected
    bad=s.begin();s.put(bad,tuple_(20,amplitude=777))
    mismatch=s.commit_id(bad,'demo-request-0001',409)
    require('different write payload' in mismatch['error']['message'],'identity payload collision was not rejected')
    require(s.request_status(rid)['sequence']==first_result['sequence'],'rejected attempt mutated the bound result')
    require(s.query(where=[{'column':'sample','op':'=','value':20}])['rows'][0]['amplitude']==200,'rejected attempt changed committed data')
    s.call('POST',f'/transactions/{bad}/rollback',status=404)  # failed commit closed it
    # C: an identity is bound only after the write is durably committed
    unbound='demo-request-never-committed'
    failed=s.begin();s.put(failed,tuple_(21,amplitude=1))
    s.put(failed,{'sample':21,'band':'bad-type','amplitude':'not-an-int','valid':True},400)
    s.call('POST',f'/transactions/{failed}/rollback')
    s.request_status(unbound,404)
    rebound=s.begin();s.put(rebound,tuple_(21),insert=True)
    rebound_result=s.commit_id(rebound,unbound)
    require(rebound_result['replayed'] is False,'a never-published identity was treated as a replay')
    require(s.request_status(unbound)['sequence']==rebound_result['sequence'],'published identity was not registered')
    s.commit_id(s.begin(),' x',400)
    # D: concurrent attempts. Two fresh writers using the SAME request
    # identity are not two logical writes: the global commit lock serializes
    # them, the first publishes, and the second is resolved as an identity
    # replay before first-committer-wins could reject its stale snapshot.
    # Two distinct identities racing the same key still obey first-committer-
    # wins. In neither case can the logical write be published twice.
    cid='demo-request-concurrent'
    head_before=s.stats()['sequence']
    def identified_commit():
     tx=s.begin();s.put(tx,tuple_(30));return s.commit_id(tx,cid)
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(identified_commit),pool.submit(identified_commit)]
     outcomes=[job.result() for job in jobs]
    require(sum(o['replayed'] for o in outcomes)==1,'concurrent same-identity attempts must yield one publish and one replay')
    winner=next(o for o in outcomes if not o['replayed'])
    require(all(o['sequence']==winner['sequence'] for o in outcomes),'both attempts must name the same commit')
    require(s.stats()['sequence']==head_before+1,'concurrent retry consumed a second sequence')
    require(s.query(where=[{'column':'sample','op':'=','value':30}])['matched']==1,'concurrent retry duplicated the row')
    require(s.request_status(cid)['sequence']==winner['sequence'],'identity did not converge to the single publish')
    # distinct identities on the same key are still first-committer-wins
    def distinct_commit(rid):
     tx=s.begin();s.put(tx,tuple_(31));return s.commit_id(tx,rid)
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(distinct_commit,'distinct-a'),pool.submit(distinct_commit,'distinct-b')]
     raced=[]
     for job in jobs:
      try:raced.append(job.result())
      except RuntimeError as e:
       require('got 409' in str(e) and 'write conflict' in str(e),'unexpected distinct-identity failure');raced.append({'conflict':True})
    require(sum('conflict' in o for o in raced)==1,'distinct identities must keep first-committer-wins semantics')
    # E: durability across restart; the index is rebuilt from the journal
    s.stop();s.start()
    survived=s.request_status(rid)
    require(survived['sequence']==first_result['sequence'] and survived['status']=='committed','identity outcome did not survive restart')
    after_restart=s.begin();s.put(after_restart,tuple_(20))
    replayed_after=s.commit_id(after_restart,rid)
    require(replayed_after['replayed'] and replayed_after['sequence']==first_result['sequence'],'post-restart replay diverged')
    # seeded 7 rows plus the four distinct new rows (20,21,30,31); no duplicates
    require(s.query()['matched']==11,'restart replay created a duplicate write')
    # F: vacuum horizon is the durable retention deadline; expiry forces
    # data-level reconciliation rather than an uncontrolled re-write
    expiry_id='demo-request-expiry'
    expiring=s.begin();s.put(expiring,tuple_(40),insert=True)
    expiry_commit=s.commit_id(expiring,expiry_id)
    horizon=s.stats()['sequence']
    reclaimed=s.call('POST','/vacuum',{'horizon':horizon})
    require(reclaimed['horizon']==horizon,'vacuum horizon did not advance')
    expired=s.request_status(expiry_id,404)
    require('retention floor' in expired['error']['message'],'expiry did not expose the retention floor')
    require(s.stats()['request_retention_floor']>=horizon,'retention floor lagged the horizon')
    # documented client reconciliation: inspect the actual data first
    existing=s.query(where=[{'column':'sample','op':'=','value':40}])
    require(existing['matched']==1 and existing['rows'][0]['amplitude']==400,'committed data must remain queryable after identity expiry')
    # blindly repeating the same insert-only write is stopped by the PK fact
    blind=s.begin()
    blind_put=s.put(blind,tuple_(40),insert=True,status=409)
    require('primary already exists' in blind_put['error']['message'],'expired identity bypassed the primary key fact')
    s.call('POST',f'/transactions/{blind}/rollback')
    # a genuinely new write under an expired identity is a fresh attempt
    fresh=s.begin();s.put(fresh,tuple_(41),insert=True)
    fresh_commit=s.commit_id(fresh,expiry_id)
    require(fresh_commit['replayed'] is False and fresh_commit['sequence']>expiry_commit['sequence'],'expired identity was silently resurrected')
    # G: a restored instance answers identities only inside its own range
    backup=s.call('GET','/backup',raw=True);image_path=temp/'identity-backup.lmi';image_path.write_bytes(backup)
    other=Session(binary,temp/'identity-restored',image_path).start()
    other.request_status(rid,404)
    restored_rid='demo-request-after-restore'
    rtx=other.begin();other.put(rtx,tuple_(50),insert=True)
    restored_commit=other.commit_id(rtx,restored_rid)
    require(restored_commit['replayed'] is False and other.request_status(restored_rid)['sequence']==restored_commit['sequence'],'restored instance refused a new identity')
    other.stop()
    observations={'replay':retry_result,'mismatch':mismatch,'concurrent':{'same_identity':outcomes,'distinct_identity':raced},'survived':survived,'expired_floor':s.stats()['request_retention_floor'],'restored':{'old_identity':'unknown_in_restored_range','new_commit':restored_commit}}
   else:raise ValueError(name)
   return {'workflow':name,'status':'observed','observations':observations}
  finally:
   s.stop()
   if other:other.stop()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--workflow',choices=['all',*WORKFLOWS],default='all');args=parser.parse_args()
 project=pathlib.Path(__file__).resolve().parents[1];binary=project/'bin/laminad'
 if not binary.exists():raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
 names=WORKFLOWS if args.workflow=='all' else [args.workflow]
 for name in names:print(json.dumps(walkthrough(name,binary),ensure_ascii=False),flush=True)
if __name__=='__main__':main()
