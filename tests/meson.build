executable('poll-test',
	sources: ['poll_timer.d'],
	dependencies: [photon_dep],
	build_by_default: true
)

executable('curl-download',
	sources: ['curl_download.d'],
	dependencies: [photon_dep],
	build_by_default: true
)

executable('ping_pong_full_duplex',
	sources: ['ping_pong_full_duplex.d'],
	dependencies: [photon_dep],
	build_by_default: true)

memcached_dep = declare_dependency(
	link_args: '-lmemached'
)

# executable('memcached-test',
# 	sources: ['memcached_test.d'],
# 	dependencies: [photon_dep, memcached_dep],
# 	install: true,
# 	build_by_default: false,
# )