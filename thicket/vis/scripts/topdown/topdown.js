import * as d3 from 'd3';
import StackedBars from './stackedbars.js';
import VegaBoxplots from './vega_boxplots.js';

const RT = window.Roundtrip;

let data = JSON.parse(RT['topdown_data']);

let test = JSON.parse(RT['test']);

if(test){
    let VB = new VegaBoxplots(d3.select(element).select('#plot-area'), element.offsetWidth, element.offsetHeight, data.dataframe);
    VB.render();
}
else{
    let SB = new StackedBars(d3.select(element).select('#plot-area'), element.offsetWidth, element.offsetHeight, data.dataframe);
    SB.render();
}

