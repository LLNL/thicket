import { layout } from './globals';
import { store, actions } from './store';
import { getNumericalDomain, getTopLevelInclusiveMetric } from './datautil';
import * as d3 from 'd3';

export default class StackedAreaPlot{
    constructor(tag, width, height, data){
        this.svg = tag.append('svg').attr('width', width).attr("height", height);
        this.width = width;
        this.height = height;
        this.data = data;

        this.prof_reductions = [];
        this.metrics = [];

        this.x = "launchdate";

        let cali_excludes = ['node', 'nid', 'profile', 'annotation', 'name', 'spot.channel', 'mpi.function'];
        for(let md of data.metadata){
            let top_record = {};
            for(const key of Object.keys(data.dataframe[0])){
                if(!cali_excludes.includes(key)){
                    top_record[key] = getTopLevelInclusiveMetric(data, md.profile, key);
                    this.metrics.push(key);
                }
            }
            
            top_record["profile"] = md.profile;

            //adding the .x metadata
            // launchdate by default
            if(this.x == "launchdate"){
                top_record['pivot_ordinal'] = new Date(parseInt(md[this.x])*1000);
            }else{
                top_record['pivot_ordinal'] = md[this.x];
            }
            this.prof_reductions.push(top_record);
        }

        this.prof_reductions.sort((a,b)=>(a['pivot_ordinal'] > b['pivot_ordinal']))

        let ordinal_surrogate = 0;
        for(let r of this.prof_reductions){
            r[this.x] = ordinal_surrogate;
            ordinal_surrogate += 1;
        }

        this.y = this.metrics[0];

        this.setup();
    }

    setup(){
        //setup 
        let y_domain = getNumericalDomain(this.prof_reductions, this.y);
        let x_domain = getNumericalDomain(this.prof_reductions, this.x);
        this.x_scale = null;
        this.y_scale = null; 

        // if(this.x == "launchdate"){
        //     x_domain = x_domain.map(t => new Date(t*1000))
        //     this.x_scale = d3.scaleTime().domain(x_domain).range([layout.margins.left*2, this.width-layout.margins.right]);
        // }

        console.log(x_domain);
        this.x_scale = d3.scaleLinear().domain(x_domain).range([layout.margins.left*2, this.width-layout.margins.right]);
        this.y_scale = d3.scaleLinear().domain(y_domain).range([this.height-layout.margins.top, layout.margins.bottom]);

        this.svg.append('g')
            .attr('class', 'left-axis')
            .attr('transform',`translate(${layout.margins.left*2},${0})`)
            .call(d3.axisLeft().scale(this.y_scale))
            .append('text')
                .attr('class', 'label-left')
                .attr('transform', 'rotate(270)')
                .attr('y', -layout.margins.left)
                .attr('x', -this.height/2)
                .style("text-anchor", "middle")
                .style("fill", "black")
                .text(this.y);

        this.svg.append('g')
                .attr('class', 'bottom-axis')
                .attr('transform',`translate(${0},${this.height - layout.margins.bottom*2})`)
                .call(d3.axisBottom().scale(this.x_scale))
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
                .attr('class', 'area-chart');

        this.svg.append('g')
                .attr('class', 'brush-layer');        

        this.brush = d3.brushX()
                        .extent([[layout.margins.left*2, 0],[this.width-layout.margins.right, this.height - layout.margins.bottom*2]])
                        .on("start end", (e)=>{
                            let new_actives = [];
                            if(e.selection){
                                for(const record of this.prof_reductions){
                                    if((record[this.x] >= this.x_scale.invert(e.selection[0])) && 
                                        (record[this.x] <= this.x_scale.invert(e.selection[1]))
                                    ){
                                        new_actives.push(record['profile']);
                                    }
                                }
                            }
                            store.dispatch(actions.updateActiveProfiles(new_actives));
                        })

    }

    render(){

        var area_func = d3.area()
                            .x(d => {return this.x_scale(d[this.x])})
                            .y0(this.height - layout.margins.bottom*2)
                            .y1(d => {return this.y_scale(d[this.y])});

        this.svg.select('.area-chart')
                .append('path')
                .attr('d', area_func(this.prof_reductions))
                .attr('stroke', 'black')
                .attr('fill', '#0032A0');

        
        this.svg.select('.brush-layer').call(this.brush);


    }


}
