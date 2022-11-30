import * as d3 from 'd3';

export default class StackedBars{
    constructor(div, width, height, data){
        //data
        this.topdown_vars = ['any#topdown.retiring', 'any#topdown.frontend_bound', 'any#topdown.backend_bound', 'any#topdown.bad_speculation'];
        this.records = this.test_normalize(data);
        this.uniques = this.get_unique_nodes(this.records);
        this.profs = this.get_unique_profs(this.records, this.uniques[0].nid);
        this.num_profs = this.profs.length;
        this.sortvar = 'any#topdown.backend_bound';
        this.magic_ordinal = 'any#ProblemSize';
        this.ordinal_groups = this.getOrdinalGroups();
        this.grouped_records = this.getGroupedRecords();

        console.log("GRPS:", this.ordinal_groups);

        //layout 
        this.margin = 15;
        this.row_chart_margin = 5;
        this.group_margins = 10;
        this.width = width-this.margin;
        this.height = height;
        this.bar_chart_height = 60;
        
        //derived layout
        this.label_width = this.width * .2;
        this.indiv_chart_width = this.width - this.label_width;
        this.total_bar_height = this.bar_chart_height - this.margin;
        this.bar_width = (this.width - this.margin) / this.num_profs;
        this.inner_group_width = ((this.indiv_chart_width-this.label_width)/this.ordinal_groups.length) - this.group_margins;
        
        //scales
        this.chart_row_scale = d3.scaleLinear().domain([0, this.uniques.length]).range([this.margin, this.uniques.length*this.bar_chart_height]);
        this.bar_x_scale = d3.scaleLinear().domain([0, this.ordinal_groups.length]).range([this.label_width, this.indiv_chart_width]);
        this.internal_x_scale = d3.scaleLinear().domain([0, this.profs.length/this.ordinal_groups.length]).range([this.group_margins/2, this.inner_group_width]);
        this.stacked_bar_scale = d3.scaleLinear().domain([0,1]).range([0, this.total_bar_height-this.row_chart_margin]);
        this.bar_color_scale = d3.scaleOrdinal(d3.schemeTableau10).domain(this.topdown_vars);

        //dom manip
        if(div.node().nodeName == 'div'){
            this.svg = div.append('svg')
                .attr('width', width)
                .attr('height', this.uniques.length*this.bar_chart_height);
        }
        else if(div.node().nodeName == 'svg' || div.node().nodeName == 'g'){
            this.svg = div.append('g')
                .attr('width', width)
                .attr('height', this.uniques.length*this.bar_chart_height);

        }

    }

    lookup_order(nid){
        for(let n of this.row_odering_map){
            if(parseInt(n.id) == parseInt(nid)){
                return n.layout.order;
            }
        }
    }

    getGroupedRecords(){
        let nested_recs = [];
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

        console.log(this.uniques);
        return this.uniques;
    }

    getOrdinalGroups(){
        let grps = [];
        let freq = {};

        for(let r of this.records){


            if (!grps.includes(r[this.magic_ordinal])){
                freq[r[this.magic_ordinal]] = 1;
            }
            else{
                freq[r[this.magic_ordinal]] += 1;
            }
            
            
            console.log("FREQ", freq);


            if(r[this.magic_ordinal] < 100000){
                r[this.magic_ordinal] = 80000;
            }
            else if(r[this.magic_ordinal] < 200000){
                r[this.magic_ordinal] = 160000;
            }
            else if(r[this.magic_ordinal] < 400000){
                r[this.magic_ordinal] = 320000;
            }
            else if(r[this.magic_ordinal] < 800000){
                r[this.magic_ordinal] = 640000;
            }



            if (!grps.includes(r[this.magic_ordinal])){
                grps.push(r[this.magic_ordinal]);
                freq[r[this.magic_ordinal]] = 1;
            }
            else{
                freq[r[this.magic_ordinal]] += 1;
            }
        }
        console.log("FREQ", freq);
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
        this.svg.selectAll('.chart_rows')
                .data(this.uniques)
                .join(
                    (enter)=>{
                        let row = enter.append('g')
                                .attr('class', 'chart-rows')
                                .attr('transform', (_,i)=>{return `translate(${0},${this.chart_row_scale(this.lookup_order(i))})`});
                        
                        row.append('g')
                            .attr('class', 'stacked-bars-row-left-axis')
                            .attr('transform', `translate(${this.label_width},${this.row_chart_margin})`)
                            .call(d3.axisLeft(this.stacked_bar_scale).ticks(3));
                        
                        row.append('g')
                            .attr('class', 'stacked-bars-row-bottom-axis')
                            .attr('transform', `translate(0,${this.bar_chart_height-this.margin})`)
                            .call(d3.axisBottom(this.bar_x_scale).ticks(0));
                        
                        row.append('text')
                            .attr('x', 0)
                            .attr('y', 18)
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
                                    .attr('transform', (_,i)=>{return `translate(${this.internal_x_scale(i)},${this.row_chart_margin})`});

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
                                        let ret = 0;
                                        self.y_offset = self.stacked_bar_scale(d.data);
                                        return ret;
                                    }
                                    let ret = self.y_offset;
                                    self.y_offset += self.stacked_bar_scale(d.data);
                                    return ret;
                                })
                                .attr('height', function(d){
                                    return self.stacked_bar_scale(d.data);
                                })
                                .on('click', (e,d)=>{
                                    console.log(d, d.varname, d.data);
                                });
                        
                            sub_bars.each(function(_, i, a){
                                this.getBBox().height;
                            })
                        }
                    )

        // d3.selectAll('.chart-rows')
        //     .each((e, d, i)=>{
        //         console.log(e,d,i);
        //     })
    }
}