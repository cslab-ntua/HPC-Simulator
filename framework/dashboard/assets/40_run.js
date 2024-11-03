Object.assign(window.dash_clientside.clientside, {

	run_options: function(sim_type, sim_stop_condition) {
		let visible = {"display": "flex"};
		let invisible = {"display": "none"};

		if (sim_type == "Dynamic")
			return [visible, visible]
		else
			return [invisible, invisible];
	},

	run_simulation: 
	function(
		n_clicks, 
		datalogs_machine, 
		datalogs_suite, 
		generator_type, 
		generator_options, 
		cluster_clicks,
		cluster_data,
		cluster_machine,
		cluster_nodes,
		cluster_ppn,
		schedulers_data,
		schedulers,
		simulation_experiments,
		simulation_type,
		simulation_distribution,
		generator_time,
	) 
	{
		// Define the return object
		let data = {
			'datalogs-machine': datalogs_machine,
			'datalogs-suite': datalogs_suite,
			'generator-type': generator_type,
			'generator-input': undefined,
			'cluster-nodes': undefined,
			'cluster-ppn': undefined,
			'schedulers': undefined,
			'simulation-experiments': simulation_experiments,
			'simulation-type': simulation_type,
			'simulation-dynamic-condition': {
				'distribution': simulation_distribution,
				'generator-time': generator_time
			}
		};

		// Generator options definitions
		if (generator_type == 'Random Generator') {
			let gen_value_component = generator_options.props.children[1];
			data['generator-input'] = gen_value_component.props.value;
		}
		else if (generator_type == 'Dictionary Generator') {
			let gen_options = {};
			let name, value;
			for (inp_group of generator_options) {
				children = inp_group.props.children
				name = children[0].props.children;
				value = children[1].props.value;
				Object.assign(gen_options, {
					[name]: value
				});
			}
			data['generator-input'] = gen_options;
		}
		else if (generator_type == 'List Generator') {
			let enc_contents = generator_options.props.contents;
			// let data = enc_contents.split('base64,')[1];
			// let contents = atob(data);
			// console.log(contents);
			data['generator-input'] = enc_contents;
		}
		else return window.dash_clientside.no_update;

		// Cluster definitions
		if (cluster_clicks % 2 == 0 || cluster_clicks === undefined) {
			data['cluster-nodes'] = cluster_data[cluster_machine]['nodes'];
			data['cluster-ppn'] = cluster_data[cluster_machine]['ppn'];
		}
		else {
			data['cluster-nodes'] = cluster_nodes;
			data['cluster-ppn'] = cluster_ppn;
		}

		// Schedulers definitions
		let sched_obj = {};
		let row_children, checkbox, collapse;
		for (row of schedulers) {
			row_children = row.props.children;
			sched_inp = row_children[0];
			collapse_inp = row_children[1];

			checkbox = sched_inp.props.children[0].props.children[0];
			collapse = collapse_inp.props.children[0];


			// If checkbox is checked then walk through
			// the hyperparameters list and push the bundle to the
			// sched_obj
			if (checkbox.props.value === true) {

				let hyperparams = {};
				let inp_group_children, name, value, type;
				for (inp_group of collapse.props.children) {
					inp_group_children = inp_group.props.children;
					name = inp_group_children[0].props.children;
					type = inp_group_children[1].type;

					if (type === 'Input')
						value = inp_group_children[1].props.value;
					else if (type === 'Select') {
						value = inp_group_children[1].props.value;
						if (value === 'True')
							value = true;
						else
							value = false;
					}
					else
						value = undefined;

					Object.assign(hyperparams, {
						[name]: value
					});

				}

				// Find module name for scheduling algorithm
				let mod_name;
				for (mod of schedulers_data) {
					if (mod.name == checkbox.props.label) {
						mod_name = mod.module;
						break;
					}
				}

				Object.assign(sched_obj, {
					[mod_name]: hyperparams
				});

			}

		}

		console.log(sched_obj);

		data['schedulers'] = sched_obj;

		return data;

	}

})
