import { initialState } from './globals';
import { createSlice, configureStore } from '@reduxjs/toolkit'

const reduce = {
    toggleProfActive: (state, action) => {
        if(!(action.payload in state.activeProf)){
            state.activeProf[action.payload] = false;
        }

        state.activeProf[action.payload] = !state.activeProf[action.payload]; 
    },
    updateActiveProfiles: (state, action)=>{
        for(const prf in state.activeProf){
            state.activeProf[prf] = false;
        }

        for(const prf of action.payload){
            if(!(prf in state.activeProf)){
                state.activeProf[prf] = false;
            }
    
            state.activeProf[prf] = !state.activeProf[prf]; 
        }
    },
    updateCategoricalMetric: (state, action)=>{
        state.categoricalMetric = action.payload;
    },
    updateActiveDimensions: (state, action)=>{
        state.activeDimensions = action.payload;
    },
    setAxesForScatterPlot: (state, action)=>{
        // Payload: {
        //     axes: {x:"",y:""},
        //     sid: ""
        // }
        state.scatterPlotAxes[action.payload.sid] = action.payload.axes; 
    },
    setCurrentNode: (state, action) => {
        state.currentNode = action.payload;
    },
    setHighlightedProfiles: (state, action) => {
        //payload a list of profile id
        state.highlightedProfiles = action.payload;
    }

  }

const counterSlice = createSlice({
  name: 'counter',
  initialState: initialState,
  reducers: reduce
})

export const actions = counterSlice.actions

export const store = configureStore({
  reducer: counterSlice.reducer
})

