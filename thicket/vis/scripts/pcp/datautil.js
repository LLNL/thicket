
export function getCategoricalDomain(data, id){
    let domain = [];
    for(const k in data){
        let d = data[k];
        if(!(domain.includes(d[id]))){
            domain.push(d[id]);
        }
    }
    return domain
}

export function getNumericalDomain(data, id){
    let domain = [Infinity, -Infinity];
    for(const k in data){
        let d = data[k];
        domain[0] = Math.min(d[id],domain[0]);
        domain[1] = Math.max(d[id],domain[1]);
    }
    return domain;
}

export function getAggregate(op, data, grouping_key, grouping_id, data_id){
    let sum = 0;
    let cnt = 0;
    for(let d of data){
        if(d[grouping_key] == grouping_id){
            sum += d[data_id];
            cnt += 1;
        }
    }

    if(op == 'sum')
        return sum
    
    return sum/cnt
}

function getGraphRoot(graph){
    let root = null;
    for(const nid of Object.keys(graph)){
        const n = graph[nid];
        if(n.parents.length == 0 && n.children.length > 0){
            root = nid;
            break;
        }
    }
    return root;
}


export function getTopLevelInclusiveMetric(data, prof_id, data_id){
    //find root
    let root = getGraphRoot(data.graph[0]);

    //use get inclusive time
    return getInclusiveMetricForNode(root, data.dataframe, prof_id, data_id);

}

export function getInclusiveMetricForNode(nid, data, prof_id, data_id){
    for(const d of data){
        if(d['profile'] == prof_id && d['node'] == nid){
            return d[data_id];
        }
    }
}

export function makeOrdinalMapping(metadata, key){
    let test = getCategoricalDomain(metadata, key);

    if(isNaN(test[0])){
        metadata.sort((a,b) => {
            if(a[key] > b[key]){
                return 1;
            }
            if(a[key] == b[key]){
                return 0;
            }
            return -1
        });
    }
    else{
        metadata.sort((a,b) => {
            if(parseFloat(a[key]) > parseFloat(b[key])){
                return 1;
            }
            if(parseFloat(a[key]) == parseFloat(b[key])){
                return 0;
            }
            return -1
        });
    }

    let ordinal_mapping = {};
    let i = -1
    for(const mr of metadata){
        if(!Object.keys(ordinal_mapping).includes(mr[key])){
            i += 1;
        }
        ordinal_mapping[mr[key]] = i;
    }

    ordinal_mapping.domain = [0, i];

    return ordinal_mapping;
}

export function inverseMapping(obj){
    //from here: https://www.geeksforgeeks.org/how-to-invert-key-value-in-javascript-object/
    var retobj = {};
    for(var key in obj){
      retobj[obj[key]] = key;
    }
    return retobj;
  }
