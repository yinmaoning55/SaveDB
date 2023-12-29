NAME=savedb
BINDIR=../bin
GOBUILD=CGO_ENABLED=1 go build  -buildmode=plugin  -ldflags '-w -s -buildid='

ifdef Ver
	v=$(Ver)
else
	v=1
endif

all: darwin-amd64

linux:
	GOARCH=amd64 GOOS=linux $(GOBUILD) -o $(BINDIR)/$(NAME)-$@.so.1.$(v) server.go
	if [ -e  $(BINDIR)/$(NAME)-$@.so.1 ]; then rm $(BINDIR)/$(NAME)-$@.so.1; fi
	ln -s $(BINDIR)/$(NAME)-$@.so.1.$(v) $(BINDIR)/$(NAME)-$@.so.1

darwin-amd64:
	GOARCH=amd64 GOOS=darwin $(GOBUILD) -o $(BINDIR)/$(NAME)-$@.so.1.$(v) server.go
	if [ -e  $(BINDIR)/$(NAME)-$@.so.1 ]; then rm $(BINDIR)/$(NAME)-$@.so.1; fi
	ln -s $(BINDIR)/$(NAME)-$@.so.1.$(v) $(BINDIR)/$(NAME)-$@.so.1


win64:
	set GOARCH=amd64
	set GOOS=windows
	set CGO_ENABLED=1
	go build -ldflags '-w -s -buildid=' -o $(BINDIR)/$(NAME)-$@.exe *.go

releases: linux-amd64 macos-amd64  win64
	chmod +x $(BINDIR)/$(NAME)-*
	tar czf $(BINDIR)/$(NAME)-linux-amd64.tgz -C $(BINDIR) $(NAME)-linux-amd64
	gzip $(BINDIR)/$(NAME)-linux-amd64
	gzip $(BINDIR)/$(NAME)-macos-amd64
	zip -m -j $(BINDIR)/$(NAME)-win64.zip $(BINDIR)/$(NAME)-win64.exe

clean:
	rm $(BINDIR)/*so.*
