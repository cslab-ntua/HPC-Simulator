Object.assign(window.dash_clientside.clientside, {

	create_results_menu:  function(data) {

		let menu_children = [];

		let all_btn = {
			'type': 'Button',
			'namespace': 'dash_bootstrap_components',
			'props': {
				'children': 'All in one',
				'style': {'width': '100%', 'margin': '5px'},
				'href': '#all-experiments'
			}
		};

		menu_children.push(all_btn);

		let index = 0;
		for ([exp_name, exp_obj] of Object.entries(data)) {

			let exp_btn = {
				'type': 'Button',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'id': {
						'type': 'results-exp-btn',
						'index': index
					},
					'children': exp_name,
					'style': {'width': '100%', 'margin': '5px'},
					'color': 'secondary'
				}
			}

			menu_children.push(exp_btn);

			exp_id = exp_name.toLowerCase();
			exp_id = exp_id.replace(/\s+/g, '_');

			let all_jobs_throughput_btn = {
				'type': 'Button',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': 'All jobs throughput',
					'style': {'width': '100%'},
					'color': 'secondary',
					'outline': true,
					'href': '#' + exp_id + '~all-jobs-throughputs'
				}
			}

			let all_waiting_queues_btn = {
				'type': 'Button',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': 'All waiting queues',
					'style': {'width': '100%'},
					'color': 'secondary',
					'outline': true,
					'href': '#' + exp_id + '~all-waiting-queues'
				}
			}

			let all_unused_cores_btn = {
				'type': 'Button',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': 'All unused cores',
					'style': {'width': '100%'},
					'color': 'secondary',
					'outline': true,
					'href': '#' + exp_id + '~all-unused-cores'
				}
			}

			let exp_collapse_children = [{
				'type': 'Container',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': [
						all_jobs_throughput_btn,
						all_waiting_queues_btn,
						all_unused_cores_btn
					]
				}
			}];

			for ([sched_name, res_obj] of Object.entries(exp_obj)) {

				sched_id = sched_name.toLowerCase();
				sched_id = sched_id.replace(/\s+/g, '_');

				unique_id = exp_id + '~' + sched_id;

				let label = {
					'type': 'Label',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': sched_name,
						'style': {'width': '100%'}
					}
				}

				let resource_usage_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Resource usage',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~resource_usage'
					}
				}

				let gantt_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Gantt diagram',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~gantt_diagram'
					}
				}

				let jobs_utilization_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Jobs utilization',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~jobs_utilization'
					}
				}

				let cluster_history_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Cluster history',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~cluster_history'
					}
				}

				let jobs_throughput_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Jobs throughput',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~jobs_throughput'
					}
				};

				let waiting_queue_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Waiting queue',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~waiting_queue'
					}
				}

				let workload_download_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': [
							'Workload Download ',
							{
								'type': 'I',
								'namespace': 'dash_html_components',
								'props': {
									'className': 'bi bi-file-earmark-arrow-down',
								}
							},

						],
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~download-workload'
					}
				};

				if (sched_name == 'Default Scheduler') {
					exp_collapse_children.push({
						'type': 'Container',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': [
								label,
								// resource_usage_btn,
								gantt_btn,
								// jobs_throughput_btn,
								// waiting_queue_btn,
								workload_download_btn,
								cluster_history_btn
							]
						}
					})
				}
				else {
					exp_collapse_children.push({
						'type': 'Container',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': [
								label,
								// resource_usage_btn,
								gantt_btn,
								jobs_utilization_btn,
								// jobs_throughput_btn,
								// waiting_queue_btn,
								workload_download_btn,
								cluster_history_btn
							]
						}
					})
				}
				
			}

			menu_children.push({
				'type': 'Collapse',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'id': {
						'type': 'results-exp-collapse',
						'index': index
					},
					'children': exp_collapse_children,
					'style': {
						'width': '100%', 
						'margin': '5px',
						'padding': '2px',
						'border-style': 'solid', 
						'border-width': '2px',
						'border-radius': '5px'
					},
					'is_open': false
				}
			})


			// Increment the index
			index++;

		}


		return menu_children;

	},

	open_results_nav: function(n_clicks) {
		return true;
	},

	open_experiment_collapse: function(n_clicks) {
		if (n_clicks % 2 == 1)
			return true;
		else 
			return false;
	},

	create_graph: function(hash, data) {

		if (data === undefined)
			return window.dash_clientside.no_update;

		let selection = hash.replace('#', '');
		let arr = selection.split('~')

		if (arr.length == 1 && arr[0] == 'all-experiments') {
			this.create_all_graph(data);
		}
		else if (arr.length == 2) {
			// Per experiment logs
			exp_tag = arr[0]
			experiment = exp_tag.replace(/_/g, ' ')
			experiment = experiment.charAt(0).toUpperCase() + experiment.slice(1)
			if (arr[1] == 'all-jobs-throughputs') {
				this.graph_all_jobs_throughput(data[experiment])
			}
			else if (arr[1] == 'all-waiting-queues') {
				this.graph_all_waiting_queues(data[experiment])
			}
			else if (arr[1] == 'all-unused-cores') {
				this.graph_all_unused_cores(data[experiment])
			}
			else
				return window.dash_clientside.no_update;
		}
		else {

			if (arr.length != 3)
				return window.dash_clientside.no_update;

			[exp_tag, sched_tag, graph_tag] = arr;

			if (graph_tag == 'download-workload')
				return window.dash_clientside.no_update;

			experiment = exp_tag.replace(/_/g, ' ')
			experiment = experiment.charAt(0).toUpperCase() + experiment.slice(1)

			scheduler = ''
			for (sched_name of Object.keys(data[experiment])) {
				sched_name_tagged = sched_name.toLowerCase().replace(/\s+/g, '_')
				if (sched_name_tagged == sched_tag) {
					scheduler = sched_name;
					break;
				}
			}

			graph = graph_tag.replace(/_/g, ' ')
			graph = graph.charAt(0).toUpperCase() + graph.slice(1)

			// Return figure
			if (graph == "Resource usage") {
				Plotly.newPlot("results-graph", JSON.parse(data[experiment][scheduler][graph]), {'displayModeBar': false})
			}
			else if (graph == "Gantt diagram") {
				Plotly.newPlot("results-graph", JSON.parse(data[experiment][scheduler][graph]), {'displayModeBar': false})
			}
			else if (graph == "Jobs throughput") {
				this.graph_jobs_throughput(scheduler, data[experiment][scheduler][graph])
			}
			else if (graph == "Waiting queue") {
				this.graph_waiting_queue(scheduler, data[experiment][scheduler][graph])
			}
			else if (graph == "Cluster history") {
				Plotly.newPlot("results-graph", JSON.parse(data[experiment][scheduler][graph]), {'displayModeBar': false})
			}
			else if (graph == "Jobs utilization") {

				let job_names = [];
				let speedup = [];
				let turnaround = [];
				let waiting = [];

				for ([job_name, job_util] of Object.entries(data[experiment][scheduler][graph])) {
					job_names.push(job_name);
					speedup.push(job_util['speedup']);
					turnaround.push(job_util['turnaround']);
					waiting.push(job_util['waiting']);
				}

				let speedup_trace = {
					'y': speedup,
					'text': job_names,
					'name': 'Speedup',
					'boxpoints': 'all',
					'jitter': 0.2,
					'boxmean': 'sd',
					'type': 'box',
					'showlegend': false
				};

				let turnaround_trace = {
					'y': turnaround,
					'text': job_names,
					'name': 'Turnaround ratio',
					'boxpoints': 'all',
					'jitter': 0.2,
					'boxmean': 'sd',
					'type': 'box',
					'showlegend': false,
					'xaxis': 'x2',
					'yaxis': 'y2'
				};

				let waiting_trace = {
					'y': waiting,
					'text': job_names,
					'name': 'Waiting time difference',
					'boxpoints': 'all',
					'jitter': 0.2,
					'boxmean': 'sd',
					'type': 'box',
					'showlegend': false,
					'xaxis': 'x3',
					'yaxis': 'y3'
				};

				let traces = [speedup_trace, turnaround_trace, waiting_trace];

				let layout = {
					title: '<b>' + scheduler + '</b><br>Jobs utilization',
					grid: {
						rows: 1,
						columns: 3,
						pattern: 'independent'
					}
				}

				Plotly.newPlot('results-graph', traces, layout, {'displayModeBar': false});
			}
		}
	},

	graph_jobs_throughput: function(scheduler, tuple) {
		let trace = {
			'type': 'scatter',
			'x': tuple[0],
			'y': tuple[1],
			'mode': 'lines+markers'
		};
		let layout = {
			'title': '<b>Jobs throughput</b><br>' + scheduler,
			'title_x': 0.5,
			'xaxis': {'title': '<b>Time (s)</b>'},
			'yaxis': {'title': '<b>Number of finished jobs</b>'}
		};
		Plotly.newPlot('results-graph', [trace], layout, {'displayModeBar': false});
	},

	graph_all_jobs_throughput: function(data) {
		let traces = [];
		for ([scheduler, graphs] of Object.entries(data)) {
			arrays = graphs['Jobs throughput']
			traces.push({
				'type': 'scatter',
				'mode': 'lines+markers',
				'name': scheduler,
				'x': arrays[0],
				'y': arrays[1]
			})
		}
		let layout = {
			'title': '<b>All jobs throughput</b><br>',
			'title_x': 0.5,
			'xaxis': {'title': '<b>Time (s)</b>'},
			'yaxis': {'title': '<b>Number of finished jobs</b>'}
		};
		Plotly.newPlot('results-graph', traces, layout, {'displayModeBar': false});
	},

	graph_waiting_queue: function(scheduler, tuple) {
		let trace = {
			'type': 'scatter',
			'x': tuple[0],
			'y': tuple[1],
			'mode': 'lines+markers'
		};
		let layout = {
			'title': '<b>Number of jobs inside waiting queue per checkpoint</b><br>{self.cluster.scheduler.name}' + scheduler,
			'title_x': 0.5,
			'xaxis': {'title': '<b>Time (s)</b>'},
			'yaxis': {'title': '<b>Number of waiting jobs</b>'}
		};
		Plotly.newPlot('results-graph', [trace], layout, {'displayModeBar': false});
	},

	graph_all_waiting_queues: function(data) {
		let traces = [];
		for ([scheduler, graphs] of Object.entries(data)) {
			arrays = graphs['Waiting queue']
			traces.push({
				'type': 'scatter',
				'mode': 'lines+markers',
				'name': scheduler,
				'x': arrays[0],
				'y': arrays[1]
			})
		}
		let layout = {
			'title': '<b>All waiting queues</b><br>',
			'title_x': 0.5,
			'xaxis': {'title': '<b>Time (s)</b>'},
			'yaxis': {'title': '<b>Number of waiting jobs</b>'}
		};
		Plotly.newPlot('results-graph', traces, layout, {'displayModeBar': false});
	},

	graph_all_unused_cores: function(data) {
		let traces = [];
		let ymax = -1;
		for ([scheduler, graphs] of Object.entries(data)) {
			arrays = graphs['Unused cores']
			if (ymax == -1)
				ymax = arrays[1][arrays[1].length - 1];

			traces.push({
				'type': 'scatter',
				'mode': 'lines+markers',
				'name': scheduler,
				'x': arrays[0],
				'y': arrays[1]
			})
		}
		let layout = {
			'title': '<b>All unused cores</b><br>',
			'title_x': 0.5,
			'xaxis': {'title': '<b>Time (s)</b>'},
			'yaxis': {'title': '<b>Number of unused cores</b>', 'tickmode': 'array', 'tickvals': [0, ymax]}
		};
		Plotly.newPlot('results-graph', traces, layout, {'displayModeBar': false});
	},

	create_all_graph: function(data) {

		// Get all the scheduling algorithms name except the default one
		schedulers = Object.keys( data["Experiment 0"] )
		console.log(schedulers)
		schedulers.splice(schedulers.indexOf('Default Scheduler'), 1)

		let aggr_exps = 0;
		let aggr_jobs = 0;
		let traces = [];
		let avg_makespans = [];

		for (scheduler of schedulers) {

			aggr_exps = 0;

			let index = 0;
			let jobs_text = [];
			let jobs_speedups = [];
			let jobs_turnaround = [];
			let jobs_waiting = [];
			let makespans = [];

			for (exp_obj of Object.values(data)) {

				aggr_jobs = 0;
				aggr_exps++;

				exp_data = exp_obj[scheduler]['Jobs utilization'];

				for ([job_name, job_util] of Object.entries(exp_data)) {
					jobs_text.push('[exp' + index.toString() + ']' + job_name);
					jobs_speedups.push(job_util['speedup']);
					jobs_turnaround.push(job_util['turnaround']);
					jobs_waiting.push(job_util['waiting']);

					// Culminative counter of jobs
					aggr_jobs++;
				}

				makespans.push(exp_obj[scheduler]['Makespan speedup']);

				// Increment index
				index++;
				
			}

			let speedups_trace = {
				'y': jobs_speedups,
				'text': jobs_text,
				'name': scheduler,
				'boxpoints': 'all',
				'jitter': 0.2,
				'boxmean': 'sd',
				'type': 'box',
				'showlegend': false,
				'marker': {
					'opacity': 0.6
				}
			};

			let turnaround_trace = {
				'y': jobs_turnaround,
				'text': jobs_text,
				'name': scheduler,
				'boxpoints': 'all',
				'jitter': 0.2,
				'boxmean': 'sd',
				'type': 'box',
				'showlegend': false,
				'xaxis': 'x2',
				'yaxis': 'y2',
				'marker': {
					'opacity': 0.6
				}
			};

			let waiting_trace = {
				'y': jobs_waiting,
				'text': jobs_text,
				'name': scheduler,
				'boxpoints': 'all',
				'jitter': 0.2,
				'boxmean': 'sd',
				'type': 'box',
				'showlegend': false,
				'xaxis': 'x3',
				'yaxis': 'y3',
				'marker': {
					'opacity': 0.6
				}
			};

			traces.push(speedups_trace);
			traces.push(turnaround_trace);
			traces.push(waiting_trace);

			// Calculate average makespan speedup for scheduling
			// algorithm
			let sum = 0;
			makespans.forEach(mkspan_speedup => {
				sum += mkspan_speedup;
			});

			if (makespans.length > 0)
				avg_makespans.push(sum / makespans.length);
			else
				avg_makespans.push(0);

		}

		// Create trace for makespan speedups
		let makespans_trace = {
			'type': 'scatter',
			'y': avg_makespans,
			'x': schedulers,
			'name': 'Average makespan speedup',
			'marker': {
				'color': 'black'
			},
			'showlegend': false
		};

		// Add trace for average makespan speedups
		traces.push(makespans_trace);

		// Add makespan speedups for annotation
		let annotations = [];

		for (i in avg_makespans) {
			annotations.push({
				'x': i,
				'y': avg_makespans[i],
				'xref': 'x',
				'yref': 'y',
				'text': '<b>' + avg_makespans[i].toFixed(2) + '</b>',
				'font': {
					'size': 20
				}
			})
		}

		// Create update visibility for traces args
		let upd_all_traces = [];
		let upd_speedup_traces = [];
		let upd_turnaround_traces = [];
		let upd_waiting_traces = [];

		for (s in schedulers) {

			upd_all_traces.push(true);
			upd_all_traces.push(true);
			upd_all_traces.push(true);

			upd_speedup_traces.push(true);
			upd_speedup_traces.push(false);
			upd_speedup_traces.push(false);

			upd_turnaround_traces.push(false);
			upd_turnaround_traces.push(true);
			upd_turnaround_traces.push(false);

			upd_waiting_traces.push(false);
			upd_waiting_traces.push(false);
			upd_waiting_traces.push(true);
		}

		upd_all_traces.push(true);
		upd_speedup_traces.push(true);
		upd_turnaround_traces.push(false);
		upd_waiting_traces.push(false);

		let update_menu = {
			'buttons': [
				{
					'args': [
						{
							'visible': upd_all_traces,
							'xaxis': ['x', 'x2', 'x3'],
							'yaxis': ['y', 'y2', 'y3']
						},
						{
							'xaxis.domain': [0, 0.3],
							'xaxis.visible': true,
							'yaxis.visible': true,

							'xaxis2.domain': [0.35, 0.65],
							'xaxis2.visible': true,
							'yaxis2.visible': true,

							'xaxis3.domain': [0.7, 1],
							'xaxis3.visible': true,
							'yaxis3.visible': true,

							'annotations': annotations
						}
					],
					'label': 'All',
					'method': 'update'
				},
				{
					'args': [
						{
							'visible': upd_speedup_traces,
							'xaxis': ['x', 'x', 'x'],
							'yaxis': ['y', 'y', 'y']
						},
						{
							'xaxis.domain': [0, 1],
							'xaxis.visible': true,
							'yaxis.visible': true,

							'xaxis2.visible': false,
							'xaxis2.domain': [0, 0],
							'yaxis2.visible': false,

							'xaxis3.visible': false,
							'xaxis3.domain': [0, 0],
							'yaxis3.visible': false,

							'annotations': annotations
						}
					],
					'label': 'Jobs speedups',
					'method': 'update'
				},
				{
					'args': [
						{
							'visible': upd_turnaround_traces,
							'xaxis': ['x2', 'x2', 'x2'],
							'yaxis': ['y2', 'y2', 'y2']
						},
						{
							'xaxis.visible': false,
							'xaxis.domain': [0, 0],
							'yaxis.visible': false,

							'xaxis2.visible': true,
							'xaxis2.domain': [0, 1],
							'yaxis2.visible': true,

							'xaxis3.visible': false,
							'xaxis3.domain': [0, 0],
							'yaxis3.visible': false,

							'annotations': []
						}
					],
					'label': 'Jobs turnarounds',
					'method': 'update'
				},
				{
					'args': [
						{
							'visible': upd_waiting_traces,
							'xaxis': ['x3', 'x3', 'x3'],
							'yaxis': ['y3', 'y3', 'y3']
						},
						{
							'xaxis.domain': [0, 0],
							'xaxis.visible': false,
							'yaxis.visible': false,

							'xaxis2.visible': false,
							'xaxis2.domain': [0, 0],
							'yaxis2.visible': false,

							'xaxis3.visible': true,
							'xaxis3.domain': [0, 1],
							'yaxis3.visible': true,

							'annotations': []
						}
					],
					'label': 'Jobs waiting time',
					'method': 'update'
				},
			],
			'direction': 'left',
			'showactive': true,
			'type': 'buttons',
			'x': 1,
			'xanchor': 'right',
			'y': 1,
			'yanchor': 'bottom',
		};

		let suffix = aggr_exps + ' experiment(s), ' + aggr_jobs + ' jobs per experiment';
		let layout = {
			title: '<b>All experiments and scheduling algorithms</b><br>' + suffix,
			font: {size: 15},
			grid: {
				rows: 1,
				columns: 3,
				pattern: 'independent'
			},
			xaxis: {
				title: {
					text: '<b>Scheduling algorithms</b>', 
					font: {size: 16}
				},
				tickfont: {size: 14}
			},
			yaxis: {
				title: {
					text: '<b>Jobs and average makespan speedups</b>',
					font: {size: 16}
				},
				tickfont: {size: 14}
			},
			xaxis2: {
				title: {
					text: '<b>Scheduling algorithms</b>',
					font: {size: 16}
				},
				tickfont: {size: 14}
			},
			yaxis2: {
				title: {
					text: '<b>Jobs turnaround ratio</b>',
					font: {size: 16}
				},
				tickfont: {size: 14}
			},
			xaxis3: {
				title: {
					text: '<b>Scheduling algorithms</b>',
					font: {size: 16}
				},
				tickfont: {size: 14}
			},
			yaxis3: {
				title: {
					text: '<b>Jobs waiting time difference</b>',
					font: {size: 16}
				},
				tickfont: {size: 14}
			},
			updatemenus: [update_menu],
			annotations: annotations
		};

		Plotly.newPlot("results-graph", traces, layout, {'autosizable': true, 'displayModeBar': false});

	},

	results_nav_isopen: function(isopen) {
		if (isopen)
			return {'filter': 'blur(2px) grayscale(50%)'};
		else
			return {};
	},

	results_modal_isopen: function(n_clicks, m_clicks) {

		let triggered = dash_clientside.callback_context.triggered[0]

		if (triggered === undefined)
			return window.dash_clientside.no_update;

		let triggered_id = triggered['prop_id'].split('.')[0]

		if (triggered_id == 'results-close-btn')
			return false;
		else
			return true;
	},

	results_download_graph: function(n_clicks) {
		graph_elem = document.getElementById('results-graph');
		updatemenus = graph_elem.layout.updatemenus;
		graph_elem.layout.updatemenus = [];

		Plotly.downloadImage('results-graph', {
			'format': 'png',
			'height': 1080,
			'width': 1920,
			'filename': 'newplot'
		});
		
		graph_elem.layout.updatemenus = updatemenus;
	},

	results_download_workload: function(hash, data) {
		if (data === undefined)
			return window.dash_clientside.no_update;

		let selection = hash.replace('#', '');
		let arr = selection.split('~')

		if (arr.length != 3)
			return window.dash_clientside.no_update;
		else {
			[exp_tag, sched_tag, graph_tag] = arr;

			if (graph_tag != 'download-workload')
				return window.dash_clientside.no_update;

			experiment = exp_tag.replace(/_/g, ' ')
			experiment = experiment.charAt(0).toUpperCase() + experiment.slice(1)

			scheduler = ''
			for (sched_name of Object.keys(data[experiment])) {
				sched_name_tagged = sched_name.toLowerCase().replace(/\s+/g, '_')
				if (sched_name_tagged == sched_tag) {
					scheduler = sched_name;
					break;
				}
			}

			let workload = data[experiment][scheduler]["Workload"]
			return {
				'content': workload,
				'filename': exp_tag + '.' + sched_tag + '.workload'
			}
		}
	}

})
