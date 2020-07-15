module github.com/filecoin-project/sector-storage

go 1.13

require (
	github.com/detailyang/go-fallocate v0.0.0-20180908115635-432fa640bd2e
	github.com/elastic/go-sysinfo v1.3.0
	github.com/filecoin-project/filecoin-ffi v0.0.0-20200326153646-e899cc1dd072
	github.com/filecoin-project/go-bitfield v0.0.2-0.20200518150651-562fdb554b6e
	github.com/filecoin-project/go-fil-commcid v0.0.0-20200208005934-2b8bd03caca5
	github.com/filecoin-project/go-paramfetch v0.0.2-0.20200218225740-47c639bab663
	github.com/filecoin-project/specs-actors v0.6.1
	github.com/filecoin-project/specs-storage v0.1.1-0.20200622113353-88a9704877ea
	github.com/filecoin-project/storage-fsm v0.0.0-20200625160832-379a4655b044
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.7.4
	github.com/hashicorp/go-multierror v1.0.0
	github.com/ipfs/go-cid v0.0.5
	github.com/ipfs/go-ipfs-files v0.0.7
	github.com/ipfs/go-log v1.0.3
	github.com/ipfs/go-log/v2 v2.0.3
	github.com/mattn/go-isatty v0.0.9 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.3
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543
)

replace github.com/filecoin-project/filecoin-ffi => ../lotus/extern/filecoin-ffi
