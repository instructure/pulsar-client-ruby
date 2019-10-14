require 'yaml'

class SessionClient
  class Protocol
    class Violation < RuntimeError; end

    class Node
      attr_writer :parent

      def initialize
        @parent = nil
        @next = End
      end

      def next
        if @next == End && !@parent.nil?
          @parent.next
        else
          @next
        end
      end

      def append(other, visited={})
        visited[self.object_id] = true
        if @next == End
          raise ArgumentError if other.nil?
          @next = other
        elsif !visited[@next.object_id]
          @next.append(other, visited)
        end
      end

      def dump_me(indent, i, visited)
        visited[self.object_id] = i
        puts "%3d: %s%s" % [i, "  " * indent, self.class]
      end

      def dump_next(indent, i, visited)
        if @next && @next != End
          if visited[@next.object_id]
            puts "     %s(recurse to %d)" % ["  " * indent, visited[@next.object_id]]
          else
            i = @next.dump(indent, i + 1, visited)
          end
        end
        i
      end

      def dump(indent = 0, i = 0, visited = {})
        dump_me(indent, i, visited)
        dump_next(indent, i, visited)
      end

      def matches(spec)
        case spec
        when Array
          !spec.empty? && matches_immediate(spec.first) &&
            (@next == End ? spec.length == 1 : @next.matches(spec[1..-1]))
        else
          matches_immediate(spec)
        end
      end

      def spec
        head_spec = spec_immediate
        tail_spec = @next == End ? nil : @next.spec
        case tail_spec
        when nil then head_spec
        when Array then [head_spec, *tail_spec]
        else [head_spec, tail_spec]
        end
      end

      def matches_immediate(spec)
        raise NotImplemented
      end

      def spec_immediate
        raise NotImplemented
      end

      def dual
        raise NotImplemented
      end

      def select(label)
        raise Protocol::Violation
      end

      def branch
        raise Protocol::Violation
      end

      def send(value)
        raise Protocol::Violation
      end

      def receive
        raise Protocol::Violation
      end
    end

    End = :token

    class Send < Node
      def matches_immediate(spec)
        spec.is_a?(Hash) && spec.keys == ["send"]
      end

      def spec_immediate
        # TODO
        {"send": "string"}
      end

      def dual
        root = Receive.new
        root.append(@next.dual) unless @next == End
        root
      end

      def send(value)
        yield value
        self.next
      end
    end

    class Receive < Node
      def matches_immediate(spec)
        spec.is_a?(Hash) && spec.keys == ["recv"]
      end

      def spec_immediate
        # TODO
        {"recv": "string"}
      end

      def dual
        root = Send.new
        root.append(@next.dual) unless @next == End
        root
      end

      def receive
        value = yield
        return value, self.next
      end
    end

    class Select < Node
      def initialize(mapping)
        super()
        @mapping = mapping
        @mapping.values.each do |value|
          value.parent = self unless value == End
        end
      end

      def follow(label)
        @mapping[label.to_s]
      end

      def select(label)
        selection = follow(label)
        raise Protocol::Violation unless selection
        yield label
        selection
      end

      def matches_immediate(spec)
        spec.is_a?(Hash) && spec.keys == ["select"] && spec["select"].is_a?(Hash) &&
          spec["select"].keys.map(&:to_s).sort == @mapping.keys.map(&:to_s).sort &&
          spec["select"].all? do |label, subspec|
            subprotocol = follow(label)
            subprotocol == End ?
              (subspec == [] || subspec.nil?) :
              subprotocol.matches(subspec)
          end
      end

      def spec_immediate
        subspecs = {}
        @mapping.each { |label, subprotocol| subspecs[label.to_s] = (subprotocol == End ? nil : subprotocol.spec) }
        {"select": subspecs}
      end

      def dual
        duals = {}
        @mapping.each do |key, value|
          duals[key] = value == End ? End : value.dual
        end
        root = Branch.new(duals)
        root.append(@next.dual) unless @next == End
        root
      end

      def dump(indent = 0, i = 0, visited = {})
        dump_me(indent, i, visited)
        j = i
        @mapping.each do |key, value|
          if value == End
            puts "     %s%s (noop)" % ["  " * (indent + 1), key]
          else
            puts "     %s%s" % ["  " * (indent + 1), key]
            j = value.dump(indent + 2, j + 1, visited)
          end
        end
        dump_next(indent, j, visited)
      end
    end

    class Branch < Node
      def initialize(mapping)
        super()
        @mapping = mapping
        @mapping.values.each do |value|
          value.parent = self unless value == End
        end
      end

      def follow(label)
        @mapping[label.to_s]
      end

      def branch
        label = yield
        selection = follow(label)
        raise Protocol::CoViolation unless selection
        return label, selection
      end

      def matches_immediate(spec)
        spec.is_a?(Hash) && spec.keys == ["branch"] && spec["branch"].is_a?(Hash) &&
          spec["branch"].keys.map(&:to_s).sort == @mapping.keys.map(&:to_s).sort &&
          spec["branch"].all? do |label, subspec|
            subprotocol = follow(label)
            subprotocol == End ?
              (subspec == [] || subspec.nil?) :
              subprotocol.matches(subspec)
          end
      end

      def spec_immediate
        subspecs = {}
        @mapping.each { |label, subprotocol| subspecs[label.to_s] = (subprotocol == End ? nil : subprotocol.spec) }
        {"branch": subspecs}
      end

      def dual
        duals = {}
        @mapping.each do |key, value|
          duals[key] = value == End ? End : value.dual
        end
        root = Select.new(duals)
        root.append(@next.dual) unless @next == End
        root
      end

      def dump(indent = 0, i = 0, visited = {})
        dump_me(indent, i, visited)
        j = i
        @mapping.each do |key, value|
          if value == End
            puts "     %s%s (noop)" % ["  " * (indent + 1), key]
          else
            puts "     %s%s" % ["  " * (indent + 1), key]
            j = value.dump(indent + 2, j + 1, visited)
          end
        end
        dump_next(indent, j, visited)
      end
    end

    def self.load(filename)
      # for now, only yaml filename allowed
      spec = YAML.load(File.read(filename))
      interpret(spec)
    end

    def self.interpret(spec)
      case spec
      when nil then End
      when [] then End
      when Array # non-empty
        root = interpret(spec.shift)
        root.append(interpret(spec))
        root
      when Hash
        if spec.keys.size != 1
          raise ArgumentError, "only single-key hashes recognized"
        end
        key = spec.keys.first
        value = spec[key]
        case key
        when "send" then Send.new # value ignored for now
        when "recv" then Receive.new # value ignored for now
        when "select"
          unless value.is_a?(Hash) && !value.empty? && value.keys.all?{ |k| k.is_a?(String) }
            raise ArgumentError, "value for `select` should be a non-empty Hash from labels to specs"
          end
          interpreted = Hash[value.map{ |k, subspec| [k, interpret(subspec)] }]
          Select.new(interpreted)
        when "branch"
          unless value.is_a?(Hash) && !value.empty? && value.keys.all?{ |k| k.is_a?(String) }
            raise ArgumentError, "value for `branch` should be a non-empty Hash from labels to specs"
          end
          interpreted = Hash[value.map{ |k, subspec| [k, interpret(subspec)] }]
          Branch.new(interpreted)
        else
          raise ArgumentError, "only `send`, `recv`, `select`, and `branch` primitives allowed"
        end
      else
        raise ArgumentError, "unrecognized spec: #{spec.inspect}"
      end
    end
  end
end
