{
  description = "Lady Glass - a cloud OCR pipeline written in Go";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-darwin" ];
      forAllSystems = f:
        nixpkgs.lib.genAttrs systems (system:
          f {
            pkgs = import nixpkgs { inherit system; };
          });
    in
    {
      devShells = forAllSystems ({ pkgs }: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            go
            gopls
            golangci-lint
            gotools

            awscli2
            aws-sam-cli
            localstack

            jq
            just
            tree
          ];

          shellHook = ''
            echo "Lady Glass dev shell"
            echo "Go: $(go version)"
          '';
        };
      });
    };
}
