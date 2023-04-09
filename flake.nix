{
  description = "maelstrom development and test environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/master";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }: flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs { inherit system; };

      maelstromDir = builtins.fetchTarball {
        url = "https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2";
        sha256 = "1hkczlbgps3sl4mh6hk49jimp6wmks8hki0bqijxsqfbf0hcakwq";
      };

      maelstrom = pkgs.writeShellApplication {
        name = "maelstrom";
        runtimeInputs = with pkgs; [ jre graphviz gnuplot ];
        text = ''
          exec java -Djava.awt.headless=true -jar "${maelstromDir}/lib/maelstrom.jar" "$@"
        '';
      };

      maelstromEcho = pkgs.buildGoPackage {
        name = "maelstrom-echo";
        src = pkgs.fetchFromGitHub
          {
            owner = "jepsen-io";
            repo = "maelstrom";
            rev = "v0.2.3";
            sha256 = "1hkczlbgps3sl4mh6hk49jimp6wmks8hki0bqijxsqfbf0hcakwq";
          };
        goPackagePath = "github.com/jepsen-io/maelstrom";
      };

      maelstromTest = pkgs.writeShellApplication
        {
          name = "maelstrom-test";
          runtimeInputs = [ maelstrom ];
          text = ''
            maelstrom test -w echo \
            --bin ${maelstromEcho}/bin/maelstrom-echo \
            --node-count 1 \
            --time-limit 10
          '';
        };
    in
    {
      devShell = pkgs.mkShell {
        name = "maelstrom";
        buildInputs = with pkgs; [
          # maelstrom dependencies
          maelstrom
          maelstromTest

          # rust dependencies
          cargo
          rustc
          rust-analyzer
          rustfmt
          clippy
        ];
      };
    });
}
