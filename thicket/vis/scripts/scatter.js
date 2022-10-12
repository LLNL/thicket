import { layout } from './globals';
import { store, actions } from './store';
import { getNumericalDomain, getCategoricalDomain, getInclusiveMetricForNode, makeOrdinalMapping, inverseMapping } from './datautil';
import * as d3 from 'd3';


export default class ScatterPlot{
    constructor(tag, width, height, data, id){
        const state = store.getState();

        this.id = id;
        this.svg = tag.append('svg').attr('width', width).attr("height", height+12);
        this.width = width;
        this.height = height;
        this.data = data;

        this.prof_reductions = [];
        this.metrics = [];
        this.categories = [];

        this.categorical_metric = '';
        this.color_scale = d3.scaleOrdinal(d3.schemeTableau10);



        //get cols on dataset
        let cali_excludes = ['node', 'nid', 'profile', 'annotation', 'name', 'spot.channel', 'mpi.function', 'function'];
        for(let md of data.metadata){
            let record = {};
            for(const key of Object.keys(data.dataframe[0])){
                if(!cali_excludes.includes(key)){
                    this.metrics.push(key);
                } else {
                    this.categories.push(key);
                }
                record[key] = getInclusiveMetricForNode(state.currentNode, data.dataframe, md.profile, key);
            }

            //**** OPTIMIZATION NOTE ****
            //consider replacing this with only using the dimensions passed by
            for(const key of Object.keys(md)){
                record[key] = md[key];
            }
            
            this.prof_reductions.push(record);

        }

        if(id == "OSP"){
            this.x = Object.keys(this.data.metadata[0])[0];
            this.metadata_x = true;
        }
        else{
            this.x = this.metrics[0];
            this.metadata_x = false;
        }        
        this.y = this.metrics[0];


        store.dispatch(actions.setAxesForScatterPlot({"sid":this.id, "axes":{"x":this.x, "y":this.y}}));
        
        this.setup();
        this.render();
    }

    setup(){
        //setup 
        let chart_margin_left = layout.margins.left*2;
        let x_domain = [];
        this.ordinal_map = {};
        if(this.metadata_x == true){
            this.ordinal_map = makeOrdinalMapping(this.data.metadata, this.x);
            x_domain = this.ordinal_map.domain;
        }
        else{
            x_domain = getNumericalDomain(this.prof_reductions, this.x); 
        }
        let y_domain = getNumericalDomain(this.prof_reductions, this.y);

        this.x_scale = d3.scaleLinear().domain(x_domain).range([chart_margin_left, this.width-layout.margins.right]);
        this.y_scale = d3.scaleLinear().domain(y_domain).range([this.height-layout.margins.top, layout.margins.bottom]);

        this.color_scale = d3.scaleOrdinal(d3.schemeTableau10);

        this.svg.append('g')
            .attr('class', 'left-axis')
            .attr('transform',`translate(${chart_margin_left},${0})`)
            .call(d3.axisLeft().scale(this.y_scale)) 
                .append('text')
                .attr('class', 'label-left')
                .attr('transform', 'rotate(270)')
                .attr('y', -layout.margins.left)
                .attr('x', -this.height/2)
                .style("text-anchor", "middle")
                .style("fill", "black")
                .text(this.y)

        
        let x_axis = d3.axisBottom().scale(this.x_scale);
        if(this.id == "OSP"){
            x_axis = d3.axisBottom().scale(this.x_scale).tickValues(Object.keys(this.ordinal_map));
        }

        this.svg.append('g')
                .attr('class', 'bottom-axis')
                .attr('transform',`translate(${0},${this.height - layout.margins.bottom*2})`)
                .call(x_axis)
                    .append('text')
                    .attr('class', 'label-bottom')
                    .text(this.x)
                    .style("text-anchor", "middle")
                    .style("fill", "black")
                    .attr('x', this.width/2)
                    .attr('y', layout.margins.bottom*2);

         //setting up layers
        // so things are drawn in the right order
        this.svg.append('g')
                .attr('class', 'plot-chart');

        this.svg.append('g')
                .attr('class', 'brush-layer');

        
        this.brush = d3.brush()
            .extent([[0, 0],[this.width, this.height - layout.margins.bottom*2]])
            .on("start end", (e)=>{
                let clicked = this;
                d3.selectAll('.brush-layer')
                    .each(function(){
                        let curr = d3.select(this);
                        if(curr != d3.select(clicked)){
                            curr.call(d3.brush().clear);
                        }
                    })

                let new_actives = [];
                if(e.selection){
                    if(this.metadata_x){
                        for(const record of this.prof_reductions){
                            if(
                                (this.ordinal_map[record[this.x]] >= this.x_scale.invert(e.selection[0][0])) && 
                                (this.ordinal_map[record[this.x]] <= this.x_scale.invert(e.selection[1][0])) &&
                                (record[this.y] <= this.y_scale.invert(e.selection[0][1])) &&
                                (record[this.y] >= this.y_scale.invert(e.selection[1][1]))
                            ){
                                new_actives.push(record['profile']);
                            }
                        }
                    }
                    else{
                        for(const record of this.prof_reductions){
                            if(
                                (record[this.x] >= this.x_scale.invert(e.selection[0][0])) && 
                                (record[this.x] <= this.x_scale.invert(e.selection[1][0])) &&
                                (record[this.y] <= this.y_scale.invert(e.selection[0][1])) &&
                                (record[this.y] >= this.y_scale.invert(e.selection[1][1]))
                            ){
                                new_actives.push(record['profile']);
                            }
                        }
                    }
                }
                store.dispatch(actions.updateActiveProfiles(new_actives));
            });
    }

    render(){
        const state = store.getState();
        const self = this;
        this.categorical_metric = state.categoricalMetric;

        this.x = state.scatterPlotAxes[this.id].x;
        this.y = state.scatterPlotAxes[this.id].y;

        //this should be a continious sequential scale
        // but add to the bottom domain 1SD of the domain?
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


        let x_domain = [];
        if(this.metadata_x == true){
            this.ordinal_map = makeOrdinalMapping(this.data.metadata, this.x);
            this.inverse_ordinal = inverseMapping(this.ordinal_map);
            x_domain = this.ordinal_map.domain;
        }
        else{
            x_domain = getNumericalDomain(this.prof_reductions, this.x); 
        }
        let y_domain = getNumericalDomain(this.prof_reductions, this.y);

        this.x_scale.domain(x_domain);
        this.y_scale.domain(y_domain);

        let laxis = this.svg.select('.left-axis')
            .call(d3.axisLeft().scale(this.y_scale));
        laxis.select('.label-left').text(this.y);
        
        
        let x_axis = d3.axisBottom().scale(this.x_scale);
        if(this.id == "OSP"){
            x_axis = d3.axisBottom().scale(this.x_scale).tickFormat((d)=>{return this.inverse_ordinal[d];});
        }

        let baxis = this.svg.select('.bottom-axis')
            .call(x_axis)
            .call( g => {
                g.selectAll('.tick')
                .each( function(d,i){
                    if(i%2 == 0){
                        d3.select(this)
                        .select('text')
                        .attr('transform', `translate(0, 12)`);
                    }
                })
            });        
        baxis.select('.label-bottom')
                .text(this.x)
                .attr('transform', 'translate(0,12)');



        this.svg.select('.plot-chart')
            .selectAll('.point-grp')
            .data(this.prof_reductions)
            .join( 
                (enter)=>{
                    let pt_grp = enter.append('g')
                        .attr('class', 'point-grp')
                        .attr('transform', (d)=>{
                            if(this.metadata_x){
                                return `translate(${this.x_scale(this.ordinal_map[d[this.x]])},${this.y_scale(d[this.y])})`
                            }
                            return `translate(${this.x_scale(d[this.x])},${this.y_scale(d[this.y])})`
                        });
                    
                    pt_grp.append('circle')
                        .attr('class', 'point')
                        .attr('r', 5)
                        .attr('fill', (d)=>{
                            if(state['activeProf'][d.profile]){
                                if(self.categorical_metric == ''){
                                    return d3.schemeTableau10[0];
                                }
                                return self.color_scale(d[self.categorical_metric])
                            }
                            return '#0032A0';
                        });

                    return pt_grp;
                },
                (update)=>{
                    update
                    .transition()
                    .duration(500)
                    .attr('transform', (d)=>{
                        if(this.metadata_x){
                            return `translate(${this.x_scale(this.ordinal_map[d[this.x]])},${this.y_scale(d[this.y])})`
                        }
                        return `translate(${this.x_scale(d[this.x])},${this.y_scale(d[this.y])})`
                    });

                    update.selectAll('.point')
                        .attr('fill', (d)=>{
                            if(state['activeProf'][d.profile]){
                                if(self.categorical_metric == ''){
                                    return d3.schemeTableau10[0];
                                }
                                return self.color_scale(d[self.categorical_metric])
                            }
                            return '#AAA';
                        });

                    return update;
                }
            )

        this.svg.select('.brush-layer').call(this.brush);
    }

}