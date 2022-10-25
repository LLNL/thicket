const HtmlWebpackPlugin = require('html-webpack-plugin');
const path = require('path');

module.exports = {
    module:{
        rules:[
            {
                test: /\.css$/i,
                use: ["style-loader", "css-loader"]
            },
            {
                test: /\.(js|jsx)$/,
                exclude: /node_modules/,
                loader: 'babel-loader',
                options:{
                    cwd: path.resolve(__dirname),
                    presets:["@babel/preset-env"]
                }
            }
        ]
    },
    entry: {
        pcp: [path.resolve(__dirname,'scripts/pcp/pcp.js')],
        topdown: [path.resolve(__dirname,'scripts/topdown/topdown.js')]
    },
    output: {
        publicPath: path.resolve(__dirname, 'static/'),
        filename: '[name]_bundle.js',
        path: path.resolve(__dirname, 'static/')
        // filename: '[name]_bundle.js',
        // path: path.resolve(__dirname, 'static')
    },
    optimization: {
        minimize: false
    },
    plugins:[
        new HtmlWebpackPlugin({
            template: 'templates/pcp.html',
            chunks: ['pcp'],
            filename: 'pcp_bundle.html'
        }),
        new HtmlWebpackPlugin({
            template: 'templates/topdown.html',
            chunks: ['topdown'],
            filename: 'topdown_bundle.html'
        })
    ],
    mode: 'production'
}