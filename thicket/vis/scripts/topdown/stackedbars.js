export default class StackedBars{
    constructor(div, width, height, data){
        this.svg = div.append('svg')
                .attr('width', width)
                .attr('height', height);
        
        console.log(data);
    }

    render(){

    }
}