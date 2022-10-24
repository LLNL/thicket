
import { store } from './store';
import { layout } from './globals';

import * as d3 from "d3";


class TreeTable{
    constructor(div, width, height, tree_model, table_model){
        this.tree_width = .3*width;
        this.table_width = width - this.tree_width;
        this.height = height;
        this.svg = div.append('svg').attr('height', height).attr('width', width);

        this.tree = tree_model;
        this.indented_tree = tree_model.indented_tree();

        this.x_scale = d3.scaleLinear().range([layout.margins.left, this.tree_width]).domain([0, tree_model.depth]);
        this.y_scale = d3.scaleLinear().range([layout.margins.top, height]).domain([0, Object.values(this.indented_tree).length]);

        this.pre_render();
    }

    link_path(node, parent){
        //array of 3 coords
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
            .attr('transform', `translate(${this.tree_width+layout.margins.left},${0})`)

        this.render();
        
    }

    render(){
        const self = this;
        this.tree_grp.selectAll('.nodes')
                    .data(Object.values(this.indented_tree))
                    .join(
                        function (enter){
                            let node = enter.append('g')
                                        .attr('class','nodes')
                                        .attr('transform', (d)=>`translate(${self.x_scale(d.layout.depth)},${self.y_scale(d.layout.order)})`);

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
                                .style("opacity", 1)
                        }
                    )

    }
}

class TreeModel{
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

window.onload = function(){
    d3.json("http://localhost:8000/test_area/data.json").then(function(data){
        // console.log(data);
        setup(data);
    });
};

let state = {
    active_prof: {}
}


function setup(data){
    let tree_model = new TreeModel(data.graph[0]);
    let table_model = null;

    let tree_max_w = window.innerWidth*.7;
    let tree_max_h = window.innerHeight*.9;
    let tree_div = d3.select("#treetable");

    const treetable = new TreeTable(tree_div, tree_max_w, tree_max_h, tree_model, table_model);


    //Bind render to store updates
    store.subscribe(() => treetable.render())
}