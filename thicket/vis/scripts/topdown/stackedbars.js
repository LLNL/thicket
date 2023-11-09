import * as d3 from 'd3';

export default class StackedBars{
    constructor(div, width, height, data, external_scales){
        //data
        this.topdown_vars = ['Retiring', 'Frontend bound', 'Backend bound', 'Bad speculation'];
        this.nice_vars  = ['Retiring', 'Frontend Bound', 'Backend Bound', 'Bad Speculation'];
        this.records = this.test_normalize(data);
        this.uniques = this.get_unique_nodes(this.records);
        this.profs = this.get_unique_profs(this.records, this.uniques[0].nid);
        this.num_profs = this.profs.length;
        this.sortvar = 'Backend bound';
        this.magic_ordinal = 'any#ProblemSize';
        this.ordinal_groups = this.getOrdinalGroups();
        this.grouped_records = this.getGroupedRecords();
        this.longest_string = this.getLongestStringWidth(this.uniques);
        this.external_scales = external_scales;


        //layout 
        this.margin = 15;
        this.row_chart_margin = 5;
        this.group_margins = 10;
        this.width = width;
        this.height = height;
        this.bar_chart_height = 60;
        this.relatve_svg_offset = d3.select('#plot-area').node().getBoundingClientRect();


        this.reset_scales();

        //dom manip
        if(div.node().nodeName == 'div'){
            this.svg = div.append('svg')
                .attr('width', width)
                .attr('height', this.height);
        }
        else if(div.node().nodeName == 'svg' || div.node().nodeName == 'g'){
            this.svg = div.append('g')
                .attr('width', width)
                .attr('height', this.height);
        }

        this.tooltip = d3.select('#plot-area').select('svg').append('g')
                .attr('id', 'bars-tooltip')
                .attr('visibility', 'hidden');
            
        this.tooltip.append('rect')
                .attr('fill', 'rgb(200,200,200)')
                .attr('width', 140)
                .attr('height', 15);

        this.tooltip.append('text')
                    .attr('fill', 'black')
                    .attr('x', 3)
                    .attr('y', 13)
                    .attr('font-family', 'monospace')
                    .attr('font-size', 12);

    }

    reset_scales(){
           //derived layout
           this.label_width = this.longest_string*9; //width determined hurestically 
           this.indiv_chart_width = this.width-80;
           this.inner_group_width = ((this.indiv_chart_width - this.label_width)/this.ordinal_groups.length);
           this.total_bar_height = this.bar_chart_height - this.margin;
           this.bar_width = this.inner_group_width / (this.num_profs/this.ordinal_groups.length);
           
           //scales
           this.chart_row_scale = d3.scaleLinear().domain([0, this.uniques.length]).range([0, this.height]);
           this.bar_x_scale = d3.scaleLinear().domain([0, this.ordinal_groups.length]).range([this.label_width, this.indiv_chart_width]);
           this.internal_x_scale = d3.scaleLinear().domain([0, this.num_profs/this.ordinal_groups.length]).range([this.group_margins/2, this.inner_group_width]);
           this.stacked_bar_scale = d3.scaleLinear().domain([0, 1]).range([0, this.total_bar_height]);
           this.stacked_bar_axis = d3.scaleLinear().domain([1, 0]).range([0, this.total_bar_height]);
           this.bar_color_scale = d3.scaleOrdinal(d3.schemeTableau10).domain(this.topdown_vars);
           this.legend_scale = d3.scaleLinear().domain([0, this.topdown_vars.length+1]).range([0, this.width]);
    }

    getLongestStringWidth(list){
        var longest = 0;
        for(var s in list){
            longest = Math.max(longest, list[s].name.length);
        }
        return longest;
    }

    lookup_order(nid){
        for(let n of this.row_odering_map){
            if(parseInt(n.id) == parseInt(nid)){
                return n.layout.order;
            }
        }
    }

    getGroupedRecords(){
        //get rows first
        for(let n of this.uniques){
            let row = this.records.filter(r => {return r.nid == n.nid});
            row.sort((f1,f2)=>{return f1[this.magic_ordinal] > f2[this.magic_ordinal]});
            let gr = [];
            for( let g of this.ordinal_groups ){
                gr.push({'ordinal': g, 'data':row.filter(r => {return r[this.magic_ordinal] == g})});
            }
            n['records'] = gr;
        }

        return this.uniques;
    }

    getOrdinalGroups(){
        let grps = [];
        let freq = {};

        for(let r of this.records){

            if (!grps.includes(r[this.magic_ordinal])){
                grps.push(r[this.magic_ordinal]);
                freq[r[this.magic_ordinal]] = 1;
            }
            else{
                freq[r[this.magic_ordinal]] += 1;
            }
        }
        return grps;
    }

    test_normalize(data){
        for(let r of data){
            let sum = 0;
            for(let v of this.topdown_vars){
                sum += r[v];
            }
            // console.log("SUM", sum);
            if(sum > 1){
                for(let v of this.topdown_vars){
                    r[v] = r[v]/sum;
                }
            }
        }

        return data;
    }

    get_unique_profs(dataframe, id){
        let profs = [];
        let node_records = dataframe.filter(r => {return r.nid == id})

        for(const r of node_records){
            profs.push(r.profile);
        }

        return profs;
    }

    get_unique_nodes(dataframe){
        let uniques = [];
        let test = [];

        for(let r of dataframe){
            if(!test.includes(r.nid)){
                let node = {'nid':r.nid,'name':r.name};
                uniques.push(node);
                test.push(r.nid);
            }
        }

        return uniques;
    }

    set_height(h){
        this.height = h;
        this.svg.attr('height', this.height);
    }

    set_width(w){
        this.width = w;
        this.svg.attr('width', this.width);
    }

    set_row_scale(rs){
        this.chart_row_scale = rs;
    }
    
    set_row_ordering_map(om){
        console.log("Oderning map:", om);
        this.row_odering_map = om;
    }

    render(){
        const self = this;
        this.y_offset = 0;

        //update width scales
        this.reset_scales();


        this.svg.selectAll('.chart_rows')
                .data(this.uniques)
                .join(
                    (enter)=>{
                        let row = enter.append('g')
                                .attr('class', 'chart-rows')
                                .attr('transform', (_,i)=>{return `translate(${0},${this.external_scales.tree_y_scale(this.lookup_order(i)) - 15})`});
                        
                        row.append('g')
                            .attr('class', 'stacked-bars-row-left-axis')
                            .attr('transform', `translate(${this.label_width},${0})`)
                            .call(d3.axisLeft(this.stacked_bar_axis).ticks(3));
                        
                        row.append('g')
                            .attr('class', 'stacked-bars-row-bottom-axis')
                            .attr('transform', `translate(0,${this.bar_chart_height-this.margin})`)
                            .call(d3.axisBottom(this.bar_x_scale).ticks(0));
                        
                        row.append('text')
                            .attr('x', 0)
                            .attr('y', 17)
                            .text(d=>{return d.name})
                            
                        return row;
                    }
                )
                .selectAll('.ordinal-groups')
                    .data(d => {
                        d.records.sort((r1,r2) => {return r1['ordinal'] > r2['ordinal']})
                        return d.records
                    })
                    .join(
                        (enter) => {
                            let grp = enter.append('g')
                                            .attr('class', 'ordinal-groups')
                                            .attr('transform', (_,i)=>{return `translate(${this.bar_x_scale(i)},0)`});

                            grp.append('text')
                                .attr('class', 'bottom-axis-group-label')
                                .attr('text-anchor', 'middle')
                                .attr('transform', (_, i) =>{ return `translate(${(this.inner_group_width/2) + (this.group_margins/2)}, ${this.bar_chart_height})`})
                                .text(d=>{return `${d.ordinal}`});

                            return grp;
                        }
                    )
                .selectAll('.stacked-bar')
                    .data(d => {
                        let recs = d.data;
                        recs.sort((f1,f2)=>{return f1[this.sortvar] > f2[this.sortvar]});
                        return recs;
                    })
                    .join(
                        (enter) => {
                            //do rect for each topdown var
                            let bar = enter.append('g')
                                    .attr('class', 'stacked-bar')
                                    .attr('transform', (_,i)=>{return `translate(${this.internal_x_scale(i)},${0})`});

                            return bar;
                        }
                    )
                .selectAll('.bar-portion')
                    .data(d=>{
                        let pivot = [];
                        for(let v of self.topdown_vars){
                            pivot.push({'varname': v, 'data':d[v]})
                        }
                        pivot.sort((f1,f2)=>{
                            if(f1.varname == this.sortvar){
                                return 1
                            } 
                            else{
                                return 0
                            }
                        })
                        return pivot;
                    })
                    .join(
                        (enter)=>{
                            let sub_bars = enter.append('rect')
                                .attr('width', Math.min(this.bar_width, 20))
                                .attr('class', (d)=>{`${d.varname}-bar`})
                                .attr('class', 'bar-portion')
                                .attr('stroke-width', 1)
                                .attr('stroke', 'black')
                                .attr('fill', (d)=>{return self.bar_color_scale(d.varname)})
                                .attr('y', (d, i)=>{
                                    if(i == 0){
                                        self.y_offset = this.total_bar_height - self.stacked_bar_scale(d.data);
                                        return self.y_offset;
                                    }
                                    self.y_offset -= self.stacked_bar_scale(d.data);
                                    return self.y_offset;
                                })
                                .attr('height', function(d){
                                    return self.stacked_bar_scale(d.data);
                                })
                                .on('click', (e,d)=>{
                                    console.log(d, d.varname, d.data);
                                })
                                .on('mouseenter', (e, d)=>{
                                    this.relatve_svg_offset = d3.select('#plot-area').node().getBoundingClientRect();
                                    let coords = [e.x - this.relatve_svg_offset.x, e.y - this.relatve_svg_offset.y];        
                                    self.tooltip.attr('visibility', 'visible');
                                    self.tooltip.attr('transform', `translate(${coords[0]}, ${coords[1]})`);
                                    self.tooltip.select('text').text(`${d.varname}: ${Number.parseFloat(d.data).toFixed(2)}`);
                                })
                                .on('mouseout', (e, d)=>{
                                    self.tooltip.attr('visibility', 'hidden');
                                    self.tooltip.attr('transform', `translate(${0}, ${0})`);
                                });
                        
                            sub_bars.each(function(_, i, a){
                                this.getBBox().height;
                            })
                        }
                    )
        
        var legend = this.svg.append('g')
            .attr('class', 'legend')
            .attr('width', this.width)
            .attr('height', 70)
            .attr('transform', `translate(${0}, ${(this.uniques.length+1)*(this.bar_chart_height + this.margin)})`);
        
        legend.append('text')
            .attr('')

        legend.selectAll('.label-grp')
            .data(this.topdown_vars)
            .join(
                (enter)=>{
                    let label = enter.append('g')
                        .attr('class', 'label-grp')
                        .attr('transform', (d,i)=>{console.log(d, i); return `translate(${self.legend_scale(i)}, ${15})`});
                    
                    label.append('rect')
                         .attr('height', 20)
                         .attr('width', 20)
                         .attr('fill', (d)=>{return self.bar_color_scale(d)})
                    
                    label.append('text')
                        .text((d,i)=>{return self.nice_vars[i]})
                        .attr('x', 23)
                        .attr('y', 10)
                        .attr('dominant-baseline', 'middle');
                    
                }
            )

        this.set_height(this.svg.node().getBBox().height + legend.node().getBBox().height + 200);
        
        this.svg.selectAll('text').style('font-family', 'monospace');

    }
}