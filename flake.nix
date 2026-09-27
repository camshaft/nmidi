{
  description = "nmidi — RTP-MIDI over the network, plus the nmidid data-plane daemon";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      # Cross-OS: Linux (ALSA) + macOS (CoreMIDI) hosts are both supported.
      systems = [
        "aarch64-linux"
        "x86_64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system);
      pkgsFor = system: import nixpkgs { inherit system; };

      # midir links ALSA on Linux and the CoreMIDI/CoreFoundation frameworks on
      # macOS; pkg-config resolves alsa.pc.
      buildInputsFor =
        pkgs:
        if pkgs.stdenv.hostPlatform.isDarwin then
          [ pkgs.apple-sdk ]
        else
          [ pkgs.alsa-lib ];
      nativeBuildInputsFor = pkgs: [ pkgs.pkg-config ];

      nmidiPackage =
        pkgs:
        pkgs.rustPlatform.buildRustPackage {
          pname = "nmidi";
          version = "0.1.0";
          src = self;
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = nativeBuildInputsFor pkgs;
          buildInputs = buildInputsFor pkgs;
          meta = {
            description = "Network MIDI (RTP-MIDI) tools and the nmidid data-plane daemon";
            license = pkgs.lib.licenses.mit;
            mainProgram = "nmidid";
          };
        };
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
          nmidi = nmidiPackage pkgs;
        in
        {
          inherit nmidi;
          nmidid = nmidi;
          default = nmidi;
        }
      );

      devShells = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = pkgs.mkShell {
            nativeBuildInputs = (nativeBuildInputsFor pkgs) ++ [
              pkgs.cargo
              pkgs.rustc
              pkgs.rustfmt
              pkgs.clippy
            ];
            buildInputs = buildInputsFor pkgs;
          };
        }
      );

      # `services.capmesh.nmidid` — a systemd unit for the data-plane daemon,
      # so the repo is directly deployable from dotfiles via Colmena.
      nixosModules.nmidid =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.nmidid;
        in
        {
          options.services.nmidid = {
            enable = lib.mkEnableOption "the nmidid MIDI data-plane daemon";
            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.nmidid;
              defaultText = lib.literalExpression "nmidi.packages.\${system}.nmidid";
              description = "The nmidid package to run.";
            };
            socket = lib.mkOption {
              type = lib.types.str;
              default = "/run/nmidid/nmidid.sock";
              description = "Path of the Unix control socket nmidid binds.";
            };
            logLevel = lib.mkOption {
              type = lib.types.enum [
                "trace"
                "debug"
                "info"
                "warn"
                "error"
              ];
              default = "info";
              description = "Log verbosity.";
            };
          };

          config = lib.mkIf cfg.enable {
            systemd.services.nmidid = {
              description = "nmidid MIDI data-plane daemon";
              wantedBy = [ "multi-user.target" ];
              after = [ "sound.target" ];
              serviceConfig = {
                ExecStart = "${lib.getExe cfg.package} --socket ${cfg.socket} --log-level ${cfg.logLevel}";
                RuntimeDirectory = "nmidid";
                Restart = "on-failure";
                # Local-trust socket: owner/group only (CONTROL-PROTOCOL §1.1).
                UMask = "0117";
                SupplementaryGroups = [ "audio" ];
              };
            };
          };
        };
    };
}
