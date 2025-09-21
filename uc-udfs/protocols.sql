-- Databricks notebook source

-- User-defined functions to work with network protocols as defined in
-- https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml

CREATE OR REPLACE FUNCTION proto_name_to_code(name STRING) 
RETURNS INT 
COMMENT 'Maps network protocol name into a numeric code as per IANA' 
RETURN 
    CASE lower(name)
        WHEN 'hopopt' THEN 0
        WHEN 'icmp' THEN 1
        WHEN 'igmp' THEN 2
        WHEN 'ggp' THEN 3
        WHEN 'ipv4' THEN 4
        WHEN 'st' THEN 5
        WHEN 'tcp' THEN 6
        WHEN 'cbt' THEN 7
        WHEN 'egp' THEN 8
        WHEN 'igp' THEN 9
        WHEN 'bbn-rcc-mon' THEN 10
        WHEN 'nvp-ii' THEN 11
        WHEN 'pup' THEN 12
        WHEN 'argus' THEN 13
        WHEN 'emcon' THEN 14
        WHEN 'xnet' THEN 15
        WHEN 'chaos' THEN 16
        WHEN 'udp' THEN 17
        WHEN 'mux' THEN 18
        WHEN 'dcn-meas' THEN 19
        WHEN 'hmp' THEN 20
        WHEN 'prm' THEN 21
        WHEN 'xns-idp' THEN 22
        WHEN 'trunk-1' THEN 23
        WHEN 'trunk-2' THEN 24
        WHEN 'leaf-1' THEN 25
        WHEN 'leaf-2' THEN 26
        WHEN 'rdp' THEN 27
        WHEN 'irtp' THEN 28
        WHEN 'iso-tp4' THEN 29
        WHEN 'netblt' THEN 30
        WHEN 'mfe-nsp' THEN 31
        WHEN 'merit-inp' THEN 32
        WHEN 'dccp' THEN 33
        WHEN '3pc' THEN 34
        WHEN 'idpr' THEN 35
        WHEN 'xtp' THEN 36
        WHEN 'ddp' THEN 37
        WHEN 'idpr-cmtp' THEN 38
        WHEN 'tp++' THEN 39
        WHEN 'il' THEN 40
        WHEN 'ipv6' THEN 41
        WHEN 'sdrp' THEN 42
        WHEN 'ipv6-route' THEN 43
        WHEN 'ipv6-frag' THEN 44
        WHEN 'idrp' THEN 45
        WHEN 'rsvp' THEN 46
        WHEN 'gre' THEN 47
        WHEN 'dsr' THEN 48
        WHEN 'bna' THEN 49
        WHEN 'esp' THEN 50
        WHEN 'ah' THEN 51
        WHEN 'i-nlsp' THEN 52
        WHEN 'swipe' THEN 53
        WHEN 'narp' THEN 54
        WHEN 'min-ipv4' THEN 55
        WHEN 'tlsp' THEN 56
        WHEN 'skip' THEN 57
        WHEN 'ipv6-icmp' THEN 58
        WHEN 'ipv6-nonxt' THEN 59
        WHEN 'ipv6-opts' THEN 60
        WHEN 'cftp' THEN 62
        WHEN 'sat-expak' THEN 64
        WHEN 'kryptolan' THEN 65
        WHEN 'rvd' THEN 66
        WHEN 'ippc' THEN 67
        WHEN 'sat-mon' THEN 69
        WHEN 'visa' THEN 70
        WHEN 'ipcv' THEN 71
        WHEN 'cpnx' THEN 72
        WHEN 'cphb' THEN 73
        WHEN 'wsn' THEN 74
        WHEN 'pvp' THEN 75
        WHEN 'br-sat-mon' THEN 76
        WHEN 'sun-nd' THEN 77
        WHEN 'wb-mon' THEN 78
        WHEN 'wb-expak' THEN 79
        WHEN 'iso-ip' THEN 80
        WHEN 'vmtp' THEN 81
        WHEN 'secure-vmtp' THEN 82
        WHEN 'vines' THEN 83
        WHEN 'iptm' THEN 84
        WHEN 'nsfnet-igp' THEN 85
        WHEN 'dgp' THEN 86
        WHEN 'tcf' THEN 87
        WHEN 'eigrp' THEN 88
        WHEN 'ospfigp' THEN 89
        WHEN 'sprite-rpc' THEN 90
        WHEN 'larp' THEN 91
        WHEN 'mtp' THEN 92
        WHEN 'ax.25' THEN 93
        WHEN 'ipip' THEN 94
        WHEN 'micp' THEN 95
        WHEN 'scc-sp' THEN 96
        WHEN 'etherip' THEN 97
        WHEN 'encap' THEN 98
        WHEN 'gmtp' THEN 100
        WHEN 'ifmp' THEN 101
        WHEN 'pnni' THEN 102
        WHEN 'pim' THEN 103
        WHEN 'aris' THEN 104
        WHEN 'scps' THEN 105
        WHEN 'qnx' THEN 106
        WHEN 'a/n' THEN 107
        WHEN 'ipcomp' THEN 108
        WHEN 'snp' THEN 109
        WHEN 'compaq-peer' THEN 110
        WHEN 'ipx-in-ip' THEN 111
        WHEN 'vrrp' THEN 112
        WHEN 'pgm' THEN 113
        WHEN 'l2tp' THEN 115
        WHEN 'ddx' THEN 116
        WHEN 'iatp' THEN 117
        WHEN 'stp' THEN 118
        WHEN 'srp' THEN 119
        WHEN 'uti' THEN 120
        WHEN 'smp' THEN 121
        WHEN 'sm' THEN 122
        WHEN 'ptp' THEN 123
        WHEN 'isis over ipv4' THEN 124
        WHEN 'fire' THEN 125
        WHEN 'crtp' THEN 126
        WHEN 'crudp' THEN 127
        WHEN 'sscopmce' THEN 128
        WHEN 'iplt' THEN 129
        WHEN 'sps' THEN 130
        WHEN 'pipe' THEN 131
        WHEN 'sctp' THEN 132
        WHEN 'fc' THEN 133
        WHEN 'rsvp-e2e-ignore' THEN 134
        WHEN 'mobility header' THEN 135
        WHEN 'udplite' THEN 136
        WHEN 'mpls-in-ip' THEN 137
        WHEN 'manet' THEN 138
        WHEN 'hip' THEN 139
        WHEN 'shim6' THEN 140
        WHEN 'wesp' THEN 141
        WHEN 'rohc' THEN 142
        WHEN 'ethernet' THEN 143
        WHEN 'aggfrag' THEN 144
        WHEN 'nsh' THEN 145
        WHEN 'homa' THEN 146
        WHEN 'bit-emu' THEN 147
        WHEN 'reserved' THEN 255
        ELSE NULL
    END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION proto_code_to_name(code INT) 
RETURNS STRING 
COMMENT 'Maps network protocol numeric code into the name as per IANA' 
RETURN 
    CASE code
        WHEN 0 THEN 'HOPOPT'
        WHEN 1 THEN 'ICMP'
        WHEN 2 THEN 'IGMP'
        WHEN 3 THEN 'GGP'
        WHEN 4 THEN 'IPv4'
        WHEN 5 THEN 'ST'
        WHEN 6 THEN 'TCP'
        WHEN 7 THEN 'CBT'
        WHEN 8 THEN 'EGP'
        WHEN 9 THEN 'IGP'
        WHEN 10 THEN 'BBN-RCC-MON'
        WHEN 11 THEN 'NVP-II'
        WHEN 12 THEN 'PUP'
        WHEN 13 THEN 'ARGUS (deprecated)'
        WHEN 14 THEN 'EMCON'
        WHEN 15 THEN 'XNET'
        WHEN 16 THEN 'CHAOS'
        WHEN 17 THEN 'UDP'
        WHEN 18 THEN 'MUX'
        WHEN 19 THEN 'DCN-MEAS'
        WHEN 20 THEN 'HMP'
        WHEN 21 THEN 'PRM'
        WHEN 22 THEN 'XNS-IDP'
        WHEN 23 THEN 'TRUNK-1'
        WHEN 24 THEN 'TRUNK-2'
        WHEN 25 THEN 'LEAF-1'
        WHEN 26 THEN 'LEAF-2'
        WHEN 27 THEN 'RDP'
        WHEN 28 THEN 'IRTP'
        WHEN 29 THEN 'ISO-TP4'
        WHEN 30 THEN 'NETBLT'
        WHEN 31 THEN 'MFE-NSP'
        WHEN 32 THEN 'MERIT-INP'
        WHEN 33 THEN 'DCCP'
        WHEN 34 THEN '3PC'
        WHEN 35 THEN 'IDPR'
        WHEN 36 THEN 'XTP'
        WHEN 37 THEN 'DDP'
        WHEN 38 THEN 'IDPR-CMTP'
        WHEN 39 THEN 'TP++'
        WHEN 40 THEN 'IL'
        WHEN 41 THEN 'IPv6'
        WHEN 42 THEN 'SDRP'
        WHEN 43 THEN 'IPv6-Route'
        WHEN 44 THEN 'IPv6-Frag'
        WHEN 45 THEN 'IDRP'
        WHEN 46 THEN 'RSVP'
        WHEN 47 THEN 'GRE'
        WHEN 48 THEN 'DSR'
        WHEN 49 THEN 'BNA'
        WHEN 50 THEN 'ESP'
        WHEN 51 THEN 'AH'
        WHEN 52 THEN 'I-NLSP'
        WHEN 53 THEN 'SWIPE (deprecated)'
        WHEN 54 THEN 'NARP'
        WHEN 55 THEN 'Min-IPv4'
        WHEN 56 THEN 'TLSP'
        WHEN 57 THEN 'SKIP'
        WHEN 58 THEN 'IPv6-ICMP'
        WHEN 59 THEN 'IPv6-NoNxt'
        WHEN 60 THEN 'IPv6-Opts'
        WHEN 61 THEN 'Any host internal protocol'
        WHEN 62 THEN 'CFTP'
        WHEN 63 THEN 'Any local network'
        WHEN 64 THEN 'SAT-EXPAK'
        WHEN 65 THEN 'KRYPTOLAN'
        WHEN 66 THEN 'RVD'
        WHEN 67 THEN 'IPPC'
        WHEN 68 THEN 'Any distributed file system'
        WHEN 69 THEN 'SAT-MON'
        WHEN 70 THEN 'VISA'
        WHEN 71 THEN 'IPCV'
        WHEN 72 THEN 'CPNX'
        WHEN 73 THEN 'CPHB'
        WHEN 74 THEN 'WSN'
        WHEN 75 THEN 'PVP'
        WHEN 76 THEN 'BR-SAT-MON'
        WHEN 77 THEN 'SUN-ND'
        WHEN 78 THEN 'WB-MON'
        WHEN 79 THEN 'WB-EXPAK'
        WHEN 80 THEN 'ISO-IP'
        WHEN 81 THEN 'VMTP'
        WHEN 82 THEN 'SECURE-VMTP'
        WHEN 83 THEN 'VINES'
        WHEN 84 THEN 'IPTM'
        WHEN 85 THEN 'NSFNET-IGP'
        WHEN 86 THEN 'DGP'
        WHEN 87 THEN 'TCF'
        WHEN 88 THEN 'EIGRP'
        WHEN 89 THEN 'OSPFIGP'
        WHEN 90 THEN 'Sprite-RPC'
        WHEN 91 THEN 'LARP'
        WHEN 92 THEN 'MTP'
        WHEN 93 THEN 'AX.25'
        WHEN 94 THEN 'IPIP'
        WHEN 95 THEN 'MICP (deprecated)'
        WHEN 96 THEN 'SCC-SP'
        WHEN 97 THEN 'ETHERIP'
        WHEN 98 THEN 'ENCAP'
        WHEN 99 THEN 'Any private encryption scheme'
        WHEN 100 THEN 'GMTP'
        WHEN 101 THEN 'IFMP'
        WHEN 102 THEN 'PNNI'
        WHEN 103 THEN 'PIM'
        WHEN 104 THEN 'ARIS'
        WHEN 105 THEN 'SCPS'
        WHEN 106 THEN 'QNX'
        WHEN 107 THEN 'A/N'
        WHEN 108 THEN 'IPComp'
        WHEN 109 THEN 'SNP'
        WHEN 110 THEN 'Compaq-Peer'
        WHEN 111 THEN 'IPX-in-IP'
        WHEN 112 THEN 'VRRP'
        WHEN 113 THEN 'PGM'
        WHEN 114 THEN 'Any 0-hop protocol'
        WHEN 115 THEN 'L2TP'
        WHEN 116 THEN 'DDX'
        WHEN 117 THEN 'IATP'
        WHEN 118 THEN 'STP'
        WHEN 119 THEN 'SRP'
        WHEN 120 THEN 'UTI'
        WHEN 121 THEN 'SMP'
        WHEN 122 THEN 'SM (deprecated)'
        WHEN 123 THEN 'PTP'
        WHEN 124 THEN 'ISIS over IPv4'
        WHEN 125 THEN 'FIRE'
        WHEN 126 THEN 'CRTP'
        WHEN 127 THEN 'CRUDP'
        WHEN 128 THEN 'SSCOPMCE'
        WHEN 129 THEN 'IPLT'
        WHEN 130 THEN 'SPS'
        WHEN 131 THEN 'PIPE'
        WHEN 132 THEN 'SCTP'
        WHEN 133 THEN 'FC'
        WHEN 134 THEN 'RSVP-E2E-IGNORE'
        WHEN 135 THEN 'Mobility Header'
        WHEN 136 THEN 'UDPLite'
        WHEN 137 THEN 'MPLS-in-IP'
        WHEN 138 THEN 'manet'
        WHEN 139 THEN 'HIP'
        WHEN 140 THEN 'Shim6'
        WHEN 141 THEN 'WESP'
        WHEN 142 THEN 'ROHC'
        WHEN 143 THEN 'Ethernet'
        WHEN 144 THEN 'AGGFRAG'
        WHEN 145 THEN 'NSH'
        WHEN 146 THEN 'Homa'
        WHEN 147 THEN 'BIT-EMU'
        WHEN 253 THEN 'Experimentation & testing'
        WHEN 254 THEN 'Experimentation & testing'
        WHEN 255 THEN 'Reserved'
        ELSE 'Unassigned'
    END;