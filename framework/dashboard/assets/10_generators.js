window.dash_clientside = Object.assign({}, window.dash_clientside, {
	clientside: {
		datalogs_events: function(n_clicks=0, m_value, s_options, s_value, data) {

			let visible = {'display': 'block'};
			let invisible = {'display': 'none'};
			let button_classes = ['bi bi-database', 'bi bi-upload'];
			let triggered = dash_clientside.callback_context.triggered[0]

			if (triggered === undefined)
				return window.dash_clientside.no_update;

			let triggered_id = triggered['prop_id'].split('.')[0]

			if (triggered_id == 'datalogs-btn') {
				if (n_clicks % 2 == 0) {
					return [button_classes[0], 
						m_value, 
						Object.keys(data[m_value]), 
						"All",
						visible, 
						visible, 
						invisible];
				}
				else {
					return [button_classes[1], 
						m_value, 
						s_options, 
						s_value,
						invisible, 
						invisible, 
						visible];
				}
			}
			else {
				return [button_classes[0], 
					m_value, 
					Object.keys(data[m_value]), 
					"All", 
					visible, 
					visible, 
					invisible];
			}
		},

		generator_options: function(gen_value, machine, suite, data) {
			/*
			 * Return should be the children under the Options
			 * element item in Generator
			 * */

			// Get id of actor who triggered the event
			let triggered = dash_clientside.callback_context.triggered[0]
			let triggered_id;
			if (triggered !== undefined)
				triggered_id = triggered['prop_id'].split('.')[0];
			else
				triggered_id = undefined

			if (triggered_id == 'datalogs-machines-select') {
				if (gen_value == 'Dictionary Generator') {
					let children = [];

					for (workload of data[machine][suite]) {
						let inpgtext = {
							'type': 'InputGroupText',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': workload,
								'style': {
									'width': '50%'
								}
							}
						};

						let inpval = {
							'type': 'Input',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'value': 0,
								'min': 0,
								'type': 'number'
							}
						};

						let inpgroup = {
							'type': 'InputGroup',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [inpgtext, inpval]
							}
						};

						children.push(inpgroup);
					}

					return children;

				}
				else return window.dash_clientside.no_update;
			}
			else if (triggered_id == 'datalogs-suites-select') {
				if (gen_value == 'Dictionary Generator') {
					let children = [];

					for (workload of data[machine][suite]) {
						let inpgtext = {
							'type': 'InputGroupText',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': workload,
								'style': {
									'width': '50%'
								}
							}
						};

						let inpval = {
							'type': 'Input',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'value': 0,
								'min': 0,
								'type': 'number'
							}
						};

						let inpgroup = {
							'type': 'InputGroup',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [inpgtext, inpval]
							}
						};

						children.push(inpgroup);
					}

					return children;

				}
				else return window.dash_clientside.no_update;
			}
			else {
				if (gen_value == 'List Generator') {
					let upload_btn = {
						'type': 'Upload',
						'namespace': 'dash_core_components',
						'props': {
							'id': 'upload-workload',
							'children': [{
								'type': 'Button',
								'namespace': 'dash_bootstrap_components',
								'props': {
									'children': 'Upload file',
									'style': {'width': '100%'}
								}
							}]
						}
					};

					return upload_btn;
				}

				else if (gen_value == 'Dictionary Generator') {

					let children = [];

					for (workload of data[machine][suite]) {
						let inpgtext = {
							'type': 'InputGroupText',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': workload,
								'style': {
									'width': '50%'
								}
							}
						};

						let inpval = {
							'type': 'Input',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'value': 0,
								'min': 0,
								'type': 'number'
							}
						};

						let inpgroup = {
							'type': 'InputGroup',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [inpgtext, inpval]
							}
						};

						children.push(inpgroup);
					}

					return children;
				}
				else if (gen_value == 'Random Generator') {
					let inpgtext = {
						'type': 'InputGroupText',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': 'Number of jobs',
							'style': {
								'width': '50%'
							}
						}
					};

					let inpval = {
						'type': 'Input',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'value': 1,
							'min': 1,
							'type': 'number'
						}
					};

					let inpgroup = {
						'type': 'InputGroup',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': [inpgtext, inpval]
						}
					};

					return inpgroup;
				}
				else return window.dash_clientside.no_update;
			}
			
		}
	}
})
