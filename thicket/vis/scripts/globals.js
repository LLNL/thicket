export let RT = window.Roundtrip

export const initialState = {
    activeProf: {},
    overviewMetrics: [],
    categoricalMetric: "",
    scatterPlotAxes: {},
    currentNode: 0,
    highlightedProfiles: []
}

export const layout = {
    max_width: 0,
    max_height: 0,
    margins: {
        left: 50,
        right: 50,
        top: 30,
        bottom: 15
    }
}