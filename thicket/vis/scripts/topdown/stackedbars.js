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

        //layout 
        this.margin = 15;
        this.width = width-this.margin;
        this.height = height;
        this.bar_chart_height = 100;
        
        //derived layout
        this.label_width = this.width * .2;
        this.indiv_chart_width = this.width;
        this.total_bar_height = this.bar_chart_height - this.margin;
        this.bar_width = (this.width - this.margin) / this.num_profs;
        
        //scales
        this.chart_row_scale = d3.scaleLinear().domain([0, this.uniques.length]).range([this.margin, this.uniques.length*this.bar_chart_height]);
        this.bar_x_scale = d3.scaleLinear().domain([0, this.num_profs]).range([this.label_width, this.indiv_chart_width]);
        this.stacked_bar_scale = d3.scaleLinear().domain([0,1]).range([0, this.total_bar_height]);
        this.bar_color_scale = d3.scaleOrdinal(d3.schemeTableau10).domain(this.topdown_vars);

        //dom manip
        this.svg = div.append('svg')
                .attr('width', width)
                .attr('height', this.uniques.length*this.bar_chart_height);

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

    render(){
        const self = this;
        this.y_offset = 0;
        this.svg.selectAll('.chart_rows')
                .data(this.uniques)
                .join(
                    (enter)=>{
                        let row = enter.append('g')
                                .attr('class', 'chart-rows')
                                .attr('transform', (_,i)=>{return `translate(${0},${this.chart_row_scale(i)})`});
                        
                        row.append('g')
                            .attr('class', 'stacked-bars-row-left-axis')
                            .attr('transform', `translate(${this.label_width},0)`)
                            .call(d3.axisLeft(this.stacked_bar_scale));
                        
                        row.append('g')
                            .attr('class', 'stacked-bars-row-bottom-axis')
                            .attr('transform', `translate(0,${this.bar_chart_height-this.margin})`)
                            .call(d3.axisBottom(this.bar_x_scale));
                        
                        row.append('text')
                            .attr('x', 0)
                            .attr('y', 25)
                            .text(d=>{return d.name})

                            
                        return row;
                    }
                )
                .selectAll('.stacked-bar')
                    .data(d => {
                        let rec = this.records.filter(rec => rec.nid == d.nid);
                        rec.sort((f1,f2)=>{return f1[this.sortvar] < f2[this.sortvar]});
                        return rec
                    })
                    .join(
                        (enter) => {
                            //do rect for each topdown var
                            let bar = enter.append('g')
                                    .attr('class', 'stacked-bar')
                                    .attr('transform', (_,i)=>{return `translate(${this.bar_x_scale(i)},0)`});

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
                                .on('mouseover', (e,d)=>{
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