#!/usr/bin/env ruby
## 
## Copyright (C) #{COPYRIGHT_YEAR}# The Regents of the University of California.
## 
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 2 of the License, or
## (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
## 


#############################################################################
## Utility methods useful for MIDAR probing scripts (estimation, resolution,
## and corroboration).
##
## $Id: midar-probing-utils.rb,v 1.1 2014/05/01 23:10:03 amogh Exp $
#############################################################################

#===========================================================================
# Record types:
#   T -- got response to probe (usually from the target)
#   N -- didn't get any response to probe
#
#   R -- got response to probe but *not* from target
#   H -- possibly hit end host or firewall
#   S -- stopped probing due to some condition (variable fields)
#
# The {R, H, S} record types only apply to the indir method.  A T record
# just means that there was a raw response to a probe; a T doesn't
# necessarily mean that the response came from the target or that the
# response is of an expected form (for example, we expect an echo reply for
# an echo request, but a port unreachble would be unexpected).  However, in
# the case of the indir method, a T response _does_ mean a response from the
# target.
#
# A field will contain the string "-" if no value is available.  Fields
# that may have "-" are reply_quoted_ttl, resp_icmp_type, resp_icmp_code,
# and resp_tcp_flags.  The probe_ipid will always be 0 (rather than "-")
# for N records.
#
# The resp_tcp_flags field contains the the raw value of the flags field
# in the TCP header.  The flags are: cwr, ece, urg, ack, psh, rst, syn, fin,
# where cwr is bit 7 and fin is bit 0.
#
# The reqnum field records the internal ID used for a probe.  The probing
# script starts the reqnum at 1 and increments the reqnum with each probe.
# The reqnum is mainly provided for debugging purposes (in studying
# response order and in matching up the low-level MperIO log records with
# the high-level result records).
#
# Starting with the Oct 2011 MIDAR run, the reqnum field also contains a
# set ID, which has the format "<set>.<subset>".  The subset IDs are
# created during subset-elim and corroboration.  The set ID is "-" during
# the discovery run.
#
# The resp_warts_id field is obsolete and will always be 0.
#
#---------------------------------------------------------------------------
#   0.  record_type = { T, R, H }
#   1.  reqnum:set_id
#   2.  target_addr
#   3.  technique = { tcp-dir, udp-dir, icmp-dir, indir+0, indir-1, indir+1 }
#   4.  probe_src_addr
#   5.  probe_dst_addr
#   6.  probe_ttl
#   7.  probe_ipid
#   8.  probe_time
#   9.  resp_src_addr
#  10.  reply_ttl
#  11.  reply_quoted_ttl
#  12.  reply_ipid
#  13.  resp_icmp_type
#  14.  resp_icmp_code
#  15.  resp_tcp_flags
#  16.  resp_warts_id
#  17.  resp_time
#---------------------------------------------------------------------------
#   0.  record_type = { N }
#   1.  reqnum
#   2.  target_addr
#   3.  technique = { tcp-dir, udp-dir, icmp-dir, indir+0, indir-1, indir+1 }
#   4.  probe_src_addr
#   5.  probe_dst_addr
#   6.  probe_ttl
#   7.  probe_ipid
#   8.  probe_time
#---------------------------------------------------------------------------
#   0.  record_type = { S }
#   1.  reqnum
#   2.  target_addr
#   3.  technique = { tcp-dir, udp-dir, indir+0, indir-1, indir+1 }
#   4.  probe_src_addr
#   5.  probe_dst_addr
#   6.  condition = { max_hit, max_nonresponse, max_ttl_expansion, early_hit }
#   7.  .... variable fields
#---------------------------------------------------------------------------
class ProgressOutput

  def self.probe_info(set_id, target, technique, result)
    # NOTE: The probe_ipid value is meaningless for a non-response.
    sprintf "%d:%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d.%06d",
      result.reqnum, set_id, target, technique,
      result.probe_src, result.probe_dest,
      result.probe_ttl, (result.probe_ipid || 0), result.tx_sec, result.tx_usec
  end

  def self.reply_info(result)
    qttl = (result.reply_qttl || "-").to_s
    icmp_type = (result.reply_icmp_type || "-").to_s
    icmp_code = (result.reply_icmp_code || "-").to_s
    tcp_flags = (result.reply_tcp_flags || "-").to_s

    sprintf "%s\t%d\t%s\t%d\t%s\t%s\t%s\t0\t%d.%06d",
      result.reply_src, result.reply_ttl, qttl, result.reply_ipid,
      icmp_type, icmp_code, tcp_flags, result.rx_sec, result.rx_usec
  end

  def self.stop_info(set_id, target, technique, result)
    sprintf "%d:%s\t%s\t%s\t%s\t%s", result.reqnum, set_id, target, technique,
      result.probe_src, result.probe_dest
  end

end


#===========================================================================

# This probes the target address directly with ICMP echo request.
class DirectICMPProbingStrategy

  attr_reader :target, :set_id, :succeeded

  def initialize(target, set_id="-", spacing=0)
    @target = target
    @set_id = set_id
    @spacing = spacing
    @succeeded = false
  end


  def duplicate
    self.dup
  end


  def execute_probe(mperio, target_id)
    mperio.ping_icmp target_id, @target, :spacing, @spacing
  end


  def handle_result(mperio, result)
    probe_info = ProgressOutput.probe_info @set_id, @target, "icmp-dir", result
    if result.responded?
      reply_info = ProgressOutput.reply_info result
      printf "T\t%s\t%s\n", probe_info, reply_info
      @succeeded = true
    else
      printf "N\t%s\n", probe_info
    end

    false  # this strategy has finished
  end


  def handle_failure(mperio, message)
    $log.err "ERROR: direct probing to %s failed: %s", @target, message
  end

end


#===========================================================================

# This probes the target address directly with TCP-ACK ping.
class DirectTCPProbingStrategy

  attr_reader :target, :set_id, :succeeded

  def initialize(target, set_id="-", spacing=0)
    @target = target
    @set_id = set_id
    @spacing = spacing
    @succeeded = false
  end


  def duplicate
    self.dup
  end


  def execute_probe(mperio, target_id)
    mperio.ping_tcp target_id, @target, :dport, 80, :spacing, @spacing
  end


  def handle_result(mperio, result)
    probe_info = ProgressOutput.probe_info @set_id, @target, "tcp-dir", result
    if result.responded?
      reply_info = ProgressOutput.reply_info result
      printf "T\t%s\t%s\n", probe_info, reply_info
      @succeeded = true
    else
      printf "N\t%s\n", probe_info
    end

    false  # this strategy has finished
  end


  def handle_failure(mperio, message)
    $log.err "ERROR: direct probing to %s failed: %s", @target, message
  end

end


#===========================================================================

# This probes the target address directly with UDP ping to a high port.
class DirectUDPProbingStrategy

  attr_reader :target, :set_id, :succeeded

  def initialize(target, set_id="-", spacing=0)
    @target = target
    @set_id = set_id
    @spacing = spacing
    @succeeded = false
  end


  def duplicate
    self.dup
  end


  def execute_probe(mperio, target_id)
    mperio.ping_udp target_id, @target, :spacing, @spacing
  end


  def handle_result(mperio, result)
    probe_info = ProgressOutput.probe_info @set_id, @target, "udp-dir", result
    if result.responded?
      reply_info = ProgressOutput.reply_info result
      printf "T\t%s\t%s\n", probe_info, reply_info
      @succeeded = true
    else
      printf "N\t%s\n", probe_info
    end

    false  # this strategy has finished
  end


  def handle_failure(mperio, message)
    $log.err "ERROR: direct probing to %s failed: %s", @target, message
  end

end


#===========================================================================

# This probes the target address indirectly with TTL-limited ICMP echo request.
class IndirectProbingStrategy

  MAX_HIT = 2
  MAX_NONRESPONSE = 3

  # Override this to 1 for estimation stage with
  #   IndirectProbingStrategy::MAX_TTL_EXPANSION = 1
  MAX_TTL_EXPANSION = 0

  # The maximum number of times to attempt TTL expansion.  This is useful,
  # for example, for restricting TTL expansion to the first round of
  # probing, where a correction to @target_hop is the most likely to
  # happen.
  MAX_TTL_EXPANSION_ATTEMPTS = 1

  # We conservatively assume we've hit (or nearly hit) an end host or
  # a firewall at the remote network edge when we see any of these ICMP
  # type-code pairs in a response.
  STOP_ICMP_TYPE_CODE = {
    [0, 0] => true,  # Echo Reply
    [0, 1] => true,  # Echo Reply (rare but happens)
    [3, 1] => true,  # Host Unreachable
    [3, 2] => true,  # Protocol Unreachable
    [3, 3] => true,  # Port Unreachable
    [3, 7] => true,  # Destination Host Unknown
    [3, 9] => true,  # Communication with Dest Net Administratively Prohibited
    [3, 10] => true, # Communication with Dest Host is Admin Prohibited
    [3, 12] => true, # Destination Host Unreachable for Type of Service
    [3, 13] => true  # Communication Administratively Prohibited
  }

  attr_accessor :expansion_attempts
  attr_reader :set_id, :target, :succeeded

  def initialize(prototype, target, destination, target_hop,
                 terminal_hop, checksum, set_id="-", spacing=0)
    @prototype = prototype
    @target = target
    @destination = destination
    @target_hop = target_hop
    @terminal_hop = terminal_hop  # == nil if unknown (gap or bogons)
    @checksum = checksum
    @set_id = set_id
    @spacing = spacing

    @expansion_attempts = 0  # number of times TTL expansion was attempted
    @offset = 0  # sequence: 0, -1, +1, -2, +2, ...
    @hit_count = 0  # number of hops where we might have hit a host or firewall
    @nonresponse_count = 0  # number of hops that lack any ICMP responses
    @succeeded = false
  end


  def duplicate
    IndirectProbingStrategy.new self, @target, @destination,
      @target_hop, @terminal_hop, @checksum, @set_id, @spacing
  end


  def update_target_hop(target_hop)
    @target_hop = target_hop
  end


  # XXX may want to lower @target_hop to be lower than @terminal_hop
  def update_terminal_hop(terminal_hop)
    return unless terminal_hop
    if @terminal_hop == nil || @terminal_hop > terminal_hop
      @terminal_hop = terminal_hop
    end
  end


  def execute_probe(mperio, target_id)
    probe_hop = @target_hop + @offset
    mperio.ping_icmp_indir target_id, @destination, probe_hop, @checksum,
      @spacing
  end


  def handle_result(mperio, result)
    if result.responded?
      return handle_response_result(mperio, result)
    else
      return handle_nonresponse_result(mperio, result) 
    end
  end


  def handle_failure(mperio, message)
    $log.err "ERROR: indirect probing to %s failed: %s", @target, message
  end


  private  #................................................................

  def handle_nonresponse_result(mperio, result)
    probe_info = ProgressOutput.probe_info @set_id, @target, technique(), result
    printf "N\t%s\n", probe_info

    @nonresponse_count += 1
    if @nonresponse_count >= MAX_NONRESPONSE
      stop_info = ProgressOutput.stop_info @set_id, @target, technique(), result
      printf "S\t%s\tmax_nonresponse\n", stop_info
      return false  # stop probing for target
    else
      if @offset > 0 && @terminal_hop == nil
        # Be conservative and halt probing in the positive direction, since
        # we might be hitting the final gap in a trace and could elicit
        # complaints.
        @terminal_hop = @target_hop + @offset
        @prototype.update_terminal_hop @terminal_hop if @prototype
      end
      return expand_ttl_offset(result)
    end
  end


  def handle_response_result(mperio, result)
    probe_info = ProgressOutput.probe_info @set_id, @target, technique(), result
    reply_info = ProgressOutput.reply_info result

    if result.reply_src == @target
      printf "T\t%s\t%s\n", probe_info, reply_info
      if @prototype
        @prototype.update_target_hop @target_hop + @offset
        @prototype.update_terminal_hop @terminal_hop
      end
      @succeeded = true
      return false  # this strategy has finished
    else
      icmp_type = result.reply_icmp_type
      icmp_code = result.reply_icmp_code
      if STOP_ICMP_TYPE_CODE[[icmp_type, icmp_code]]
        printf "H\t%s\t%s\n", probe_info, reply_info
        @hit_count += 1
        if @hit_count >= MAX_HIT
          stop_info = ProgressOutput.stop_info @set_id,
            @target, technique(), result
          printf "S\t%s\tmax_hit\n", stop_info
          return false  # stop probing for target
        else
          if @offset > 0
            # Be conservative and halt probing in the positive direction, since
            # we might have hit an end host or a firewall.
            @terminal_hop = @target_hop + @offset
            @prototype.update_terminal_hop @terminal_hop if @prototype
            return expand_ttl_offset(result)
          else # @offset <= 0, so the initial TTL was too high
            # XXX Is it worth probing in the negative direction?
            stop_info = ProgressOutput.stop_info @set_id,
              @target, technique(), result
            printf "S\t%s\tearly_hit\n", stop_info
            return false  # stop probing for target
          end
        end
      else
        printf "R\t%s\t%s\n", probe_info, reply_info
        return expand_ttl_offset(result)
      end
    end
  end


  def expand_ttl_offset(result)
    if @prototype && @offset == 0 && MAX_TTL_EXPANSION > 0
      if @prototype.expansion_attempts < MAX_TTL_EXPANSION_ATTEMPTS
        @prototype.expansion_attempts += 1
      else
        stop_info = ProgressOutput.stop_info @set_id,
          @target, technique(), result
        printf "S\t%s\tmax_ttl_expansion_attempts\n", stop_info
        return false
      end
    end

    @offset = (@offset >= 0 ? -(@offset + 1) : -@offset)
    if @offset > 0 && @terminal_hop && @target_hop + @offset >= @terminal_hop
      @offset = -(@offset + 1)   # only probe in negative direction
    end

    if @offset.abs > MAX_TTL_EXPANSION
      stop_info = ProgressOutput.stop_info @set_id,
        @target, technique(), result
      printf "S\t%s\tmax_ttl_expansion\n", stop_info
      return false  # stop probing for target
    else
      return true
    end
  end


  def technique
    sprintf "indir%+d", @offset
  end

end


#===========================================================================

class TargetID
  attr_reader :id

  def initialize
    @id = 1  # don't use id == 0
  end

  def advance
    @id += 1  # unlikely to ever wrap... famous last words
    @id
  end
end


#===========================================================================

class TCPUtils

  def self.show_tcp_flags(flags)
    print " ("
    print " fin" if (flags & 0x01) != 0
    print " syn" if (flags & 0x02) != 0
    print " rst" if (flags & 0x04) != 0
    print " psh" if (flags & 0x08) != 0
    print " ack" if (flags & 0x10) != 0
    print " urg" if (flags & 0x20) != 0
    print " ece" if (flags & 0x40) != 0
    print " cwr" if (flags & 0x80) != 0
    print " )"
  end

end
