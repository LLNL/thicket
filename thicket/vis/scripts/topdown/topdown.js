import * as d3 from 'd3';
import StackedBars from './stackedbars.js';
import VegaBoxplots from './vega_boxplots.js';
import {TreeModel, TreeTable} from '../treetable.js';
import { tree } from 'd3';

const RT = window.Roundtrip;

let data = JSON.parse(RT['topdown_data']);
let test = JSON.parse(RT['test']);
let tree_model = new TreeModel(data.graph[0]);
let tree_max_w = element.offsetWidth;
let tree_max_h = window.innerHeight*.9;
let tree_div = d3.select("#plot-area");


if(test){
    let VB = new VegaBoxplots(d3.select(element).select('#plot-area'), element.offsetWidth, element.offsetHeight, data.dataframe);
    VB.render();
}
else{
    // let SB = new StackedBars(d3.create('svg'), element.offsetWidth, element.offsetHeight, data.dataframe);
    let TB = new TreeTable(tree_div, tree_max_w, tree_max_h, tree_model, data)
    TB.render();
    // store.subscribe(() => treetable.render())
}

