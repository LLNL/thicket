import { layout } from './globals';
import StackedBars from './topdown/stackedbars'

import * as d3 from "d3";


export class TreeTable{
    constructor(div, width, height, tree_model, table_data){
        this.width = width;
        this.tree_width = .2*width;
        this.table_width = width - this.tree_width;
        this.tree = tree_model;
        this.indented_tree = tree_model.indented_tree();
        this.table_data = table_data;

        this.rowheight = 75;
        this.height = Object.values(this.indented_tree).length * this.rowheight;

        // //hack for legend until I figure out how to handle this
        // this.height += 70;

        //math.max ensures that the depth will at least be 1 for scale purposes
        this.x_scale = d3.scaleLinear().range([layout.margins.left, this.tree_width]).domain([0, Math.max(1, tree_model.depth)]);
        this.y_scale = d3.scaleLinear().range([layout.margins.top, this.height]).domain([0, Object.values(this.indented_tree).length]);
       
        this.svg = div.append('svg').attr('height', this.height + layout.margins.bottom).attr('width', width);
        this.pre_render();
    }

    link_path(node, parent){
        //array of 3 coords
        //to make angled link between nodes
        //drawn from child to parent
        const coords =[
            {x:this.x_scale(node.layout.depth), y:this.y_scale(node.layout.order)},
            {x:this.x_scale(node.layout.depth-1), y:this.y_scale(node.layout.order)},
            {x:this.x_scale(node.layout.depth-1), y:this.y_scale(parent.layout.order)},
        ]

        let line = d3.line().x(coord => coord.x).y(coord => coord.y);

        return line(coords);
    }

    pre_render(){
        this.tree_grp = this.svg.append('g')
            .attr('width', this.tree_width)
            .attr('height', this.height)
            .attr('class', 'tree-group');

        this.table_grp = this.svg.append('g')
            .attr('width', this.table_width)
            .attr('height', this.height)
            .attr('class','table-group')
            .attr('transform', `translate(${this.tree_width},${0})`)

        this.table = new StackedBars(this.table_grp, this.table_width, this.height, this.table_data.dataframe, {'tree_y_scale': this.y_scale});
        this.table.set_row_ordering_map(Object.values(this.indented_tree));
    }

    render(){
        const self = this;

        //Fix tree and table ratios for cases of one row of nodes
        this.tree_width = Math.min(this.x_scale(this.tree.depth), this.tree_width);
        this.table_width = this.width - this.tree_width;
        
        //Update elements
        this.tree_grp.attr('width', this.tree_width);
        this.table_grp.attr('width',this.table_width);
        this.table_grp.attr('transform', `translate(${this.tree_width+layout.margins.left},${0})`);
        this.table.set_width(this.table_width);

        //Update scales
        this.x_scale.range([layout.margins.left, this.tree_width]);


        this.tree_grp.selectAll('.nodes')
                    .data(Object.values(this.indented_tree))
                    .join(
                        function (enter){
                            let node = enter.append('g')
                                        .attr('class','nodes')
                                        .attr('transform', (d)=>{
                                            return `translate(${self.x_scale(d.layout.depth)},${self.y_scale(d.layout.order)})`
                                        })
                                        .on('click', function(e,d){console.log(d)});

                            node.append('circle')
                                .attr('r', 8)
                                .attr('fill', 'rgba(125,50,50,1)');
                            
                            return node;
                        }
                    )

        this.tree_grp.selectAll('.links')
                    .data(Object.values(this.indented_tree))
                    .join(
                        function (enter){
                            enter.append('path')
                                .attr('class', 'links')
                                .attr("d", (d)=>{
                                    if(d.parents.length > 0){
                                        return self.link_path(d, self.indented_tree[d.parents[0]]) //breaks with graphs
                                    }
                                })
                                .style("fill", "none")
                                .style("stroke", "rgba(125,50,50,1)")
                                .style("stroke-width", 2)
                                .style("opacity", 1);
                        }
                    )


        this.table.render();

        //adjust height to reflect changes made in table
        this.svg.attr('height', Math.max(this.height + layout.margins.bottom, this.table.height))


    }
}

export class TreeModel{
    constructor(tree_def){
        this.tree = tree_def;
        this.depth = 0;
        for(const id in this.tree){
            this.tree[id].id = id;
        }
    }

    recursive_indented_tree_layout(tree, node, order, depth){
        node.layout = {};
        node.layout.order = order;
        node.layout.depth = depth;

        this.depth = Math.max(depth, this.depth);

        let child_cnt = 1;
        let total_offset = 1;

        for(let child of node.children){
            total_offset += this.recursive_indented_tree_layout(tree, tree[child], order+total_offset, depth+1);
        }

        return total_offset;
    
    }

    indented_tree(){
        let tree_cpy = JSON.parse(JSON.stringify(this.tree));
        let nodes = [];
        let total_offset = 0;

        for(let node in tree_cpy){
            if(tree_cpy[node].parents.length == 0){
                total_offset += this.recursive_indented_tree_layout(tree_cpy, tree_cpy[node], total_offset, 0);
            }
        }
    
        return tree_cpy;
    }

}
