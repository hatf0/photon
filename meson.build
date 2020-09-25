project('Photon Libary', 'd',
    meson_version : '>=0.44',
    subproject_dir : 'third-party/photon/3rd-party',
    license : 'Boost'
)

version = meson.project_version()
message(meson.source_root())
message(meson.current_source_dir())
http_parser_sp = subproject('http-parser')

http_dep = declare_dependency(
	link_with: http_parser_sp.get_variable('lib'),
	include_directories: include_directories('../http-parser'))


subdir('src')

subdir('bench/static_http')

subdir('tests')
