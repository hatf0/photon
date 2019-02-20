photon_include_dir = include_directories('.')

photon_linux = [
	'photon/linux/support.d',
	'photon/linux/syscalls.d', 
	'photon/linux/core.d'
]

photon_windows= [
	'photon/windows/support.d',
	'photon/windows/core.d'
]

photon_common = [
	'photon/package.d',
	'photon/ds/common.d',
	'photon/ds/ring_queue.d',
	'photon/ds/intrusive_queue.d',
	'photon/ds/blocking_queue.d'
]

photon = static_library('photon',
    sources: photon_common + photon_linux,
    install: true,
    build_by_default: true
)

photon_dep = declare_dependency(
	include_directories: photon_include_dir,
	link_with: photon
)

utils = static_library('utils',
    ['utils/http_parser.d', 'utils/http_server.d', 'utils/http.c'],
    dependencies: http_dep
)

utils_dep = declare_dependency(
	include_directories: photon_include_dir,
	link_with: utils
)

utils_test = executable('utils-test',
	sources: ['utils/unit.d'],
	dependencies: utils_dep,
	build_by_default: true
)
