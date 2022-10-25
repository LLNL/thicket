import * as d3 from 'd3';
import StackedBars from './stackedbars.js';

let data = JSON.parse(RT['topdown_data'])
let SB = new StackedBars(d3.select('#plot-area'), element.offsetWidth, element.offsetHeight, data);

