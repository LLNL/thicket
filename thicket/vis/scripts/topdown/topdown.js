import * as d3 from 'd3';
import {TreeModel, TreeTable} from '../treetable.js';

const RT = window.Roundtrip;

let data = JSON.parse(RT['topdown_data']);
let tree_model = new TreeModel(data.graph[0]);
let tree_max_w = element.offsetWidth;
let tree_max_h = window.innerHeight*.9;
let tree_div = d3.select("#plot-area");


let TB = new TreeTable(tree_div, tree_max_w, tree_max_h, tree_model, data)
TB.render();
 

