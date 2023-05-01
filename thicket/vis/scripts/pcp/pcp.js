import { getCategoricalDomain, getNumericalDomain, getAggregate, getTopLevelInclusiveMetric } from './datautil';
import ScatterPlot from './scatter';
import { store, actions } from './store';
import {layout, RT} from './globals';

import * as d3 from "d3";

class ParallelCoordPlot{
    constructor(tag, width, height, data){
        this.data = data;
        this.height = height-layout.margins.bottom;
        this.svg = tag.append('svg').attr('width', width+layout.margins.left+layout.margins.right).attr('height', height+layout.margins.top+layout.margins.bottom);

        let dims = Object.keys(data.metadata[0]);
        this.valid_dims = [];
        this.all_dims = [];
        this.n_dims = [];
        this.c_dims = [];
        this.xs = {};
        this.dragging = {};
        
        this.checkbox_margin = 50;
        
        let excludes = ["profile", "launchday", "launchdate", "compilerversion"];
        for(const dim of dims){
            let invalid = false;
            if(excludes.indexOf(dim) > -1){
                invalid = true;
            }

            let c_domain = getCategoricalDomain(data.metadata, dim);

            for(const d of c_domain){
                if(d == ''){
                    invalid = true;
                }
            }
            
            c_domain = c_domain.filter(n => n != null)

            if(c_domain.length > 0 && isNaN(c_domain[0]) && !invalid){
                this.xs[dim] = d3.scalePoint()
                    .domain(c_domain)
                    .range([this.checkbox_margin, width]);
                this.valid_dims.push(dim);
                this.c_dims.push(dim);
            }
            else if(c_domain.length > 0 && !(isNaN(c_domain[0])) && !invalid){
                let n_domain = getNumericalDomain(data.metadata, dim);
                this.xs[dim] = d3.scaleLinear()
                    .domain(n_domain)
                    .range([this.checkbox_margin, width]);
                this.valid_dims.push(dim);
                this.n_dims.push(dim);       
            }
        }

        this.ys = d3.scalePoint()
                    .range([layout.margins.top, height])
                    .domain(this.valid_dims);
                    
        //aggregate vis data
        this.agg = {};
        let cali_excludes = ['node', 'nid', 'profile', 'annotation', 'name', 'spot.channel', 'mpi.function', 'function'];
        for(let md of data.metadata){
            let agg_record = {};

            for(const key of Object.keys(data.dataframe[0])){
                if(!cali_excludes.includes(key)){
                    agg_record[key] = getAggregate('avg', data.dataframe, 'profile', md.profile, key);
                }
            }
            
            this.agg[md.profile] = agg_record;
        }

        this.categorical_metric = '';
        this.color_scale = d3.scaleOrdinal(d3.schemeTableau10);
        this.all_dims = this.valid_dims;

        this.setup();

    }

    path(record, class_context) {
        let line_coords = class_context.valid_dims.map(function(dimension){
            return {x:class_context.xs[dimension](record[dimension]) + layout.margins.left, y: class_context.ys(dimension)};
        });
    
        let line = d3.line()
                    .defined(coord => !isNaN(coord.x) && !isNaN(coord.y))
                    .x(coord => coord.x)
                    .y(coord => coord.y);
        
        return line(line_coords)
    }

    determine_svg_adjustment(axes){
        let top_axis = axes.filter(function(d, i){return i == 0}).node().getBoundingClientRect();
        let next_axis = axes.filter(function(d, i){return i == 1}).node().getBoundingClientRect();

        //check for collision on top two axes elements
        if(top_axis.bottom > next_axis.top){
            let deisired_diff = top_axis.bottom - next_axis.top + 5;
            return deisired_diff * this.valid_dims.length;
        }

        return 0;
    }

    setup(){                    
        let srtDropDown = d3.select('#sortby');
        let self = this;

        srtDropDown
                .selectAll('option')
                .data(Object.keys(this.agg[Object.keys(this.agg)[0]]))
                .join(
                    function(enter){
                        return enter
                                .append('option')
                                .attr('value', (d)=>{return d})
                                .append('text')
                                    .text((d)=>{return d});
                    }
                )
        
        srtDropDown.on('change', function(_,d){
                        this.categorical_metric = this.value;
                        self.render();
                    })

            
        function dragstarted(event, d) {
            this.dragging[d] = this.ys(d);
        }

        function dragged(event, d) {
            d3.select(this).attr("cx", d.x = event.x).attr("cy", d.y = event.y);
        }

        function dragended(event, d) {
            d3.select(this).attr("stroke", null);
        }

        this.drag = d3.drag()
            .on("start", dragstarted)
            .on("drag", dragged)
            .on("end", dragended);
    }

    render(){
        this.data.metadata.sort((a,b)=>{return this.agg[a['profile']][this.categorical_metric] > this.agg[b['profile']][this.categorical_metric]})
        let state = store.getState();
        let self = this;

        if('activeDimensions' in state && state['activeDimensions'].length > 0){
            this.valid_dims = state['activeDimensions'];
        } else {
            this.valid_dims = this.all_dims;
        }

        this.ys.domain(this.valid_dims).range([layout.margins.top, this.height]);

        this.categorical_metric = state.categoricalMetric;

        
        if(this.categorical_metric != ''){
            let numerical = false;
            let color_domain = getCategoricalDomain(this.data.metadata, this.categorical_metric);
            if(!(isNaN(color_domain[0]))){
                color_domain = getNumericalDomain(this.data.metadata, this.categorical_metric);
                numerical = true;
            }

            if(!numerical){
                this.color_scale = d3.scaleOrdinal(d3.schemeTableau10).domain(color_domain);
            }else{ 
                //offset bottom domain as 20% of range to ensure no light colors
                color_domain[0] -= .3*(color_domain[1]-color_domain[0]);
                this.color_scale = d3.scaleSequential(d3.interpolateGreens).domain(color_domain);
            }
        }

        // Draw the lines
        this.svg.selectAll(".prof-line")
            .data(this.data.metadata)
            .join(
                function(enter){
                    return enter.append("path")
                        .attr("class", "prof-line")
                        .attr("d", (d)=>{return self.path(d, self)})
                        .attr("visibility", "hidden")
                        .style("fill", "none")
                        .style("stroke", (d)=>{
                            if(self.categorical_metric == ''){
                                return '#AAA';
                            }
                            return self.color_scale(d[self.categorical_metric]);
                        })
                        .style("stroke-width", 1.5)
                        .style("opacity", 1)
                        .on("mouseover", function(e, d){
                            let profs = [];
                            for(let p  of store.getState().highlightedProfiles){
                                profs.push(p);
                            }
                            profs.push(d.profile);
                            store.dispatch(actions.setHighlightedProfiles(profs));

                            d3.select(this)
                                .style("opacity", 1)
                                .style("stroke", "black")
                                .style("stroke-width", 4);
                        })
                        .on("mouseleave", function(e, d){
                            let profs = [];
                            for(let p  of store.getState().highlightedProfiles){
                                profs.push(p);
                            }
                            
                            profs = profs.filter((r)=>{r != d.profile});
                            store.dispatch(actions.setHighlightedProfiles(profs));

                            d3.select(this)
                                .style("opacity",1)
                                .style("stroke-width", 1.5)
                                .style("stroke", (d)=>{
                                    if(self.categorical_metric == ''){
                                        return '#AAA';
                                    }
                                    return self.color_scale(d[self.categorical_metric])
                                });
                        })
                },
                function(update){
                    return update
                        .attr("d", (d)=>{return self.path(d, self)})
                        .attr("visibility", (d)=>{
                            if(d['profile'] in state.activeProf && state.activeProf[d['profile']] == 1){
                                return "visible";
                            }else{
                                return "hidden";
                            }
                        })
                        .style("stroke", (d)=>{
                            if(self.categorical_metric == ''){
                                return '#AAA';
                            }
                            return self.color_scale(d[self.categorical_metric]);
                        })
                }
            )
        
        
        //rendering html outside of it's div
        d3.select('#highlighted-profiles')
            .selectAll('.profstr')
            .data(store.getState().highlightedProfiles)
            .join(
                function(enter){
                    enter
                    .append('text')
                    .attr('class','profstr')
                    .text(d=>{
                        for(let k of Object.keys(self.data.profile_mapping)){
                            if(k.includes(String(d).slice(0,11))){
                                return self.data.profile_mapping[k] + ', '
                            }
                        }
                    });
                },
                function(update){},
                function(exit){
                    exit.remove();
                }
            )

            
        // Draw the axis:
        this.svg.selectAll(".pcp-axis")
        .data(this.valid_dims)
        .join(
            function(enter){
                let axes = enter.append("g")
                    .attr('class', 'pcp-axis')
                    .attr("transform", function(d) { return `translate(${layout.margins.left},${self.ys(d)})`; });
                
                axes.each(function(d){d3.select(this).call(d3.axisBottom().scale(self.xs[d]))})
                
                axes.append("text")
                        .style("text-anchor", "middle")
                        .attr("y", -9)
                        .attr("x", self.checkbox_margin/2)
                        .text(function(d) { return d; })
                        .style("fill", "black");

                let rects = axes.append('g')
                            .on('click', function(e, d){
                                store.dispatch(actions.updateCategoricalMetric(d));                           
                            });
                
                rects.append("circle")
                    .attr("r", 15)
                    .attr("fill", "rgba(0,0,0,0)")
                    .attr('transform', 'translate(20,20)');

                rects.append('svg:image')
                    .attr('class', 'crayon-icon')
                    .attr('height', 25)
                    .attr('width', 25)
                    .attr("href", "https://i.imgur.com/1iVu6gs.png");
                
                return enter;
            },
            function(update){
                update.attr("transform", function(d) { return `translate(${layout.margins.left},${self.ys(d)})`; });

                update.each(function(d){ d3.select(this).call(d3.axisBottom().scale(self.xs[d]))})
                
                update.selectAll(".crayon-icon")
                        .attr("href", (d)=>{ 
                            if(d == state["categoricalMetric"]){
                                return "https://i.imgur.com/atsqvAl.png";
                            } else{
                                return "https://i.imgur.com/1iVu6gs.png";
                            }
                        });
            }
        )

        let adjustment = this.determine_svg_adjustment(this.svg.selectAll('.pcp-axis'));

        if(adjustment > 0){
            this.svg.attr('height', parseInt(this.svg.attr('height')) + adjustment);
            this.height += adjustment;
            this.render();
        }
    }
}

function sanitizeAndMergeData(data){
    let vis_data = JSON.parse(JSON.stringify(data.dataframe))

    for(let record of vis_data){
        for(const k in record){
            if(k.includes('#')){ 
                let nk = k.replaceAll('#', '_').replaceAll('.','_');
                record[nk] = record[k];
                delete record[k];
            }
        }
        for(let md of data.metadata){
            if(md.profile == record.profile){
                for(const key in md){
                    if(key == "launchdate"){
                        record[key] = new Date(parseInt(md[key])*1000).toISOString();
                    }
                    else{
                        record[key] = md[key];
                    }
                }
            }
        }
    }

    return data;
}

function setup(data){
    data = sanitizeAndMergeData(data);

    if(Object.keys(RT).includes('focus_node')){
        let node = JSON.parse(RT['focus_node']);
        let isFound = false;
        for(let k of Object.keys(data.graph[0])){
            if(data.graph[0][k].data.name.localeCompare(node) == 0){
                store.dispatch(actions.setCurrentNode(parseInt(k)));
                isFound = true;
            }
        }
        if(!isFound){
            console.warn("Metadata Visualization Error: Node could not be found. Defaulting to root node.")
        }
    }

    let sap_max_w = element.offsetWidth*.6;
    let sap_max_h = 200;

    let sp_max_w = element.offsetWidth - sap_max_w - layout.margins.left;
    let sp_max_h = sap_max_h;

    let pcp_layout = JSON.parse(JSON.stringify(layout));
    pcp_layout.max_width = element.offsetWidth - pcp_layout.margins.left - pcp_layout.margins.right;
    pcp_layout.min_height = element.offsetHeight*2 - pcp_layout.margins.top - pcp_layout.margins.bottom;

    let sap_div = d3.select("#profiles");
    let pcp_div = d3.select("#pcp");

    let cali_excludes = ['node', 'nid', 'profile', 'annotation', 'name', 'spot.channel', 'mpi.function', 'function'];
    let axis_opts = Object.keys(data.dataframe[0]).filter((d)=>{return !cali_excludes.includes(d)})

    d3.select("#selection-boxes")
        .style('height', '100px');

    d3.select('#selection-options-left')
        .style('width', '300px')
        .style('float', 'left');

    d3.select('#selection-options-right')
        .style('width', '300px')
        .style('float', 'right');

    d3.select('#ly')
        .on('change', (e)=>{
            let current = store.getState().scatterPlotAxes["OSP"];
            let copy = {}; 
            copy.x = current.x;
            copy.y = d3.select(e.target).node().value;
            store.dispatch(actions.setAxesForScatterPlot({
                axes: copy,
                sid: "OSP"
            }))
        })
        .selectAll('.ly-opts')
        .data(axis_opts)
        .join(
            (enter)=>{
                enter.append('option')
                    .attr('value', (d)=>{return d})
                    .text((d)=>{return d});
            }
        )
    
    d3.select('#lx')
        .on('change', (e)=>{
            let current = store.getState().scatterPlotAxes["OSP"];
            let copy = {}; 
            copy.y = current.y;
            copy.x = d3.select(e.target).node().value;
            store.dispatch(actions.setAxesForScatterPlot({
                axes: copy,
                sid: "OSP"
            }))
        })
        .selectAll('.lx-opts')
        .data(Object.keys(data.metadata[0]))
        .join(
            (enter)=>{
                enter.append('option')
                    .attr('value', (d)=>{return d})
                    .text((d)=>{return d});
            }
        )

    d3.select('#rx')
        .on('change', (e)=>{
            let current = store.getState().scatterPlotAxes["SP"];
            let copy = {}; 
            copy.y = current.y;
            copy.x = d3.select(e.target).node().value;
            store.dispatch(actions.setAxesForScatterPlot({
                axes: copy,
                sid: "SP"
            }))
        })
        .selectAll('.rx-opts')
        .data(axis_opts)
        .join(
            (enter)=>{
                enter.append('option')
                    .attr('value', (d)=>{return d})
                    .text((d)=>{return d});
            }
        )

    d3.select('#ry')
        .on('change', (e)=>{
            let current = store.getState().scatterPlotAxes["SP"];
            let copy = {}; 
            copy.x = current.x;
            copy.y = d3.select(e.target).node().value;
            store.dispatch(actions.setAxesForScatterPlot({
                axes: copy,
                sid: "SP"
            }))
        })
        .selectAll('.ry-opts')
        .data(axis_opts)
        .join(
            (enter)=>{
                enter.append('option')
                    .attr('value', (d)=>{return d})
                    .text((d)=>{return d});
            }
        )

    d3.select('#profile-names-box')
            .style('min-height', "50px")
            .style('border', '1px solid black');

    // const sap = new StackedAreaPlot(sap_div, sap_max_w, sap_max_h, data);
    const osp = new ScatterPlot(sap_div, sap_max_w, sap_max_h, data, "OSP");
    const sp = new ScatterPlot(sap_div, sp_max_w, sp_max_h, data, "SP");
    const pcp = new ParallelCoordPlot(pcp_div, pcp_layout.max_width, pcp_layout.min_height, data);

    //inital render
    osp.render();
    sp.render();
    pcp.render();

    //Bind render to store updates
    store.subscribe(() => pcp.render())
    store.subscribe(() => osp.render())
    store.subscribe(() => sp.render())
}


if(RT === undefined){
    d3.json("http://localhost:8000/test_area/data.json").then(function(data){
        setup(data);
    });
}
else{
    let data = JSON.parse(RT['thicket_ensemble'])
    let pre_selected_dims = [];

    if(Object.keys(RT).includes('metadata_dims')){
        pre_selected_dims = JSON.parse(RT['metadata_dims']);
        let real_dims = Object.keys(data.metadata[0]);
        let valid_dims = pre_selected_dims.filter(value => real_dims.includes(value));
        let invalid_dims = pre_selected_dims.filter(value => !real_dims.includes(value));
        if(invalid_dims.length > 0){
            console.warn(`The following dimensions could not be found on the metadata table: ${invalid_dims}`)
        }
        store.dispatch(actions.updateActiveDimensions(valid_dims));
    }

    //put pre selected dims in the state object

    setup(data);
}
