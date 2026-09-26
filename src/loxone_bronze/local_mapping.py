"""Stream structure candidates in Python instead of repeatedly expanding JSON in SQL."""
import json
import re
import tempfile
from pathlib import Path
UUID=re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{16}',re.I)


def candidates(payload):
    controls=payload.get('controls',{})
    rooms=payload.get('rooms') or {};cats=payload.get('cats') or {}
    stack=[(str(key),obj,'$.controls.'+str(key),None,None) for key,obj in controls.items() if isinstance(obj,dict)]
    result=set();count=0
    while stack:
        key,obj,path,parent_room,parent_cat=stack.pop();count+=1
        room=obj.get('room') if obj.get('room') is not None else parent_room
        cat=obj.get('cat') if obj.get('cat') is not None else parent_cat
        metadata=(key,obj.get('name'),obj.get('type'),room,(rooms.get(room) or {}).get('name'),cat,(cats.get(cat) or {}).get('name'))
        meanings=[]
        states=obj.get('states') or {}
        if isinstance(states,dict):
            meanings.extend((state,name,'state',1) for name,state in states.items() if isinstance(state,str))
        leaves=[(None,obj.get('details'))]
        while leaves:
            name,value=leaves.pop()
            if isinstance(value,dict):leaves.extend(value.items())
            elif isinstance(value,list):leaves.extend((str(i),v) for i,v in enumerate(value))
            elif isinstance(value,str) and UUID.fullmatch(value):meanings.append((value,name,'details',2))
        if obj.get('uuidAction') is not None:meanings.append((str(obj['uuidAction']),'uuidAction','uuidAction',3))
        meanings.append((key,'control','controlKey',4))
        for state,name,source,priority in meanings:
            result.add((state,name,*metadata,source,priority,path))
        children=obj.get('subControls') or {}
        if isinstance(children,dict):
            stack.extend((str(child),value,path+'.subControls.'+str(child),room,cat)
                         for child,value in children.items() if isinstance(value,dict))
    return result,count


def sync_local_structure(c,sc,enqueue):
    c.execute('CREATE TABLE IF NOT EXISTS mapped_structure_inputs(structure_id VARCHAR PRIMARY KEY,valid BOOLEAN)')
    row=c.execute('''SELECT b.* FROM loxone_bronze.structures b
      WHERE NOT EXISTS(SELECT 1 FROM mapped_structure_inputs p WHERE p.structure_id=b.structure_id)
      ORDER BY captured_at,structure_id LIMIT 1''').fetchone()
    if row is None:return 0
    sid,source,at,modified,raw,digest,version,ingested=row
    payload=json.loads(raw)
    valid=isinstance(payload,dict) and len(raw)>1000 and isinstance(payload.get('controls'),dict) and bool(payload['controls'])
    c.execute('BEGIN')
    try:
        if valid and not c.execute(f'SELECT 1 FROM {sc.silver}.structure_versions WHERE structure_id=?',[sid]).fetchone():
            rows,controls=candidates(payload)
            if rows:
                columns=('structure_id','structure_captured_at','structure_last_modified','state_uuid','state_name',
                  'control_uuid','control_name','control_type','room_uuid','room_name','category_uuid','category_name',
                  'mapping_source','mapping_priority','control_path')
                types={name:('TIMESTAMPTZ' if name=='structure_captured_at' else 'INTEGER' if name=='mapping_priority' else 'VARCHAR') for name in columns}
                schema=','.join("'"+name+"':'"+kind+"'" for name,kind in types.items())
                with tempfile.TemporaryDirectory(prefix='local-mapping-') as tmp:
                    path=Path(tmp)/'mapping.jsonl'
                    with path.open('w') as output:
                        for r in rows:output.write(json.dumps(dict(zip(columns,(sid,at,modified,*r))),default=str)+'\n')
                    c.execute(f"INSERT INTO {sc.silver}.state_uuid_map BY NAME SELECT * FROM read_json(?,columns={{{schema}}},format='newline_delimited')",[str(path)])
            c.execute(f'''INSERT INTO {sc.silver}.structure_versions VALUES (?,?,?,?,try_cast(? AS TIMESTAMP),?,?,?,?,?,?,?,?,?)''',
              [sid,source,at,modified,modified,digest,version,ingested,len(raw.encode()),len(payload['controls']),
               controls,len(rows),len(payload.get('rooms') or {}),len(payload.get('cats') or {})])
            enqueue(c,sc)
        c.execute('INSERT INTO mapped_structure_inputs VALUES (?,?)',[sid,bool(valid)])
        c.execute('COMMIT')
    except BaseException:
        c.execute('ROLLBACK');raise
    return 1
