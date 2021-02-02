
# Курсовой проект "конвейер"
# Выполнен на языке Ruby
# 
# Общее устройство: конвейер с узлами, имеющими входы и выходы. На вход узла может подаваться
# либо константа, либо выход одного из узлов.
# 
# Данный код работает с json-файлами, в которы на предметно-специфичном языке описана
# структура конвейера. Парсинг json-файла осуществляется в функции run_pipeline.
# Структура json-файла:
# 	В файле содержится словарь с ключами "nodes", "constants" и "links".
# 	"nodes"
# 		Словарь, в котором ключом являются имена узлов, а значениями кодовые имена классов,
# 		возможно параметризованные.
# 	"constants"
# 		Словарь, в котором ключом является имя входа одного из узлов, например
# 		"format_filename.input", а значением константа (число или строка), подаваемая на вход.
# 	"links"
# 		Словарь, в котором ключом является имя выхода одного из узлов, а значением имя входа
# 		одного из узлов, между которыми нужно установить связь, либо массив из имен входов
# 		узлов, например: "stack.output": ["saver_res.image", "enhancer.input"].
# 
# Далее по коду пояснения.

require 'set'
require 'thread'
require 'json'

$debug = true

# Image - dummy-класс, описывающий изображение и содержащий следующие данные: ширина, высота,
# название и список примененных операций @performedOperations. Работа конвейера заключается
# в передаче между узлами объектов Image и применении к ним операций. Фактически класс Image
# не хранит пиксели и операции не применяются, а лишь записываются в историю @performedOperations
# для демонстрации работы конвейера.

class Image
	def initialize(w, h)
		@w = w
		@h = h
		@title = nil
		@performedOperations = Array.new
	end
	def set_title(title)
		@title = title
	end
	def operation(op)
		@performedOperations << op
	end
	def initialize_dup(orig) #making a copy
		@w = orig.instance_variable_get("@w")
		@h = orig.instance_variable_get("@h")
		@title = orig.instance_variable_get("@title")
		@performedOperations = Array.new
		orig.instance_variable_get("@performedOperations").each do |op|
			@performedOperations << op
		end
	end
end

# $types - словарь: допустимый тип данных => функция проверки значения на соответствие этому типу

$types = {
	"STRING" => lambda { |x| x.class == String },
	"BOOL" => lambda { |x| x.class == TrueClass || x.class == FalseClass },
	"INT" => lambda { |x| x.class == Integer },
	"FLOAT" => lambda { |x| x.class == Float },
	"IMAGE" => lambda { |x| x.class == Image }
}

# Node - узел конвейера, от которого наследуются другие узлы. Параметр template_str в методе
# initialize означает, что узел может быть параметризован, см. примеры ниже по коду. Узел хранит
# список именованных входов - словарь @inputs, каждый из входов является очередью (Queue) или
# константой. Словарь @outputs содержит именованные выходы и указание входа другого узла, в
# который они ведут (см. метод connectOutput). Типы входов и выходов хранятся в словарях
# @inputTypes и @outputTypes.

class Node
	def _get_debug_name
		"{#{self.class} | #{@name}}"
	end
	
	def initialize(name, template_str = nil)
		@name = name
		if $debug then puts "Node #{_get_debug_name} created" end
	end
	
	# Принять данные на вход, добавив данные в конец очереди.
	def input(inputName, value)
		if not @inputs.has_key?(inputName) then raise "no such input " + inputName end
		typeCheckerFunc = $types[@inputTypes[inputName]]
		if not typeCheckerFunc.call(value) then raise "wrong type for input " + inputName end
		if not @inputs[inputName].is_a?(Queue) then raise "input " + inputName + " is a constant" end
		@inputs[inputName] << value
		if $debug then puts "Node #{_get_debug_name} received value #{value} to input '#{inputName}'" end
	end
	
	# Сопоставить входу константу
	def assignConstant(inputName, value)
		if not @inputs.has_key?(inputName) then raise "no such input " + inputName end
		typeCheckerFunc = $types[@inputTypes[inputName]]
		if not typeCheckerFunc.call(value) then raise "wrong type for input " + inputName end
		@inputs[inputName] = value
		if $debug then puts "Node #{_get_debug_name} uses a constant #{value} for input '#{inputName}'" end
	end
	
	# Сопоставляет выход текущего узла одному из входов другого узла, типы данных должны совпадать.
	# Сопоставление осуществляется путем добавление новой записи в словарь @outputs:
	# @outputs[outputName] << {"node" => node, "input" => inputName}
	def connectOutput(outputName, node, inputName)
		if node.nil? then raise "node is nil" end
		nodeInputs = node.instance_variable_get("@inputs")
		if not nodeInputs.has_key?(inputName) then raise "no such input '#{inputName}' for given node" end
		if not nodeInputs[inputName].is_a?(Queue) then raise "input " + inputName + " for given node is a constant" end
		nodeInputTypes = node.instance_variable_get("@inputTypes")
		receiveType = nodeInputTypes[inputName]
		outputType = @outputTypes[outputName]
		if receiveType != outputType then raise "type mismatch: " + outputType + " -> " + receiveType end
		@outputs[outputName] << {"node" => node, "input" => inputName}
		if $debug then puts "Node #{_get_debug_name} connects output '#{outputName}' to input '#{inputName}'"\
			" of node #{node._get_debug_name} (type = #{receiveType})" end
	end
	
	# Далее идут методы _hasPending, _takeFromInput и _sendOutput, которые должны использоваться классами, наследующимися
	# от Node, а также метод run, который должен быть переопределен этими классами
	
	# Есть ли данные в данном именованном входе (очереди или константе)? Считаем, что если входу сопоставлена константа,
	# то данные всегда есть.
	def _hasPending(inputName)
		inputQueue = @inputs[inputName]
		if inputQueue.is_a?(Queue)
			not inputQueue.empty?
		else
			true #this input uses constant instead of queue
		end
	end
	
	# Взять данные из выбранного входа (очереди или константы) и вернуть их. Из очереди берется только один элемент,
	# а не все сразу.
	def _takeFromInput(inputName)
		inputQueue = @inputs[inputName]
		value = nil
		if inputQueue.is_a?(Queue)
			value = inputQueue.pop
		else
			value = inputQueue #this input uses constant instead of queue
		end
		if $debug then puts "run(): Node #{_get_debug_name} takes value #{value} from input '#{inputName}'" end
		value
	end
	
	# Отправить данные на выход узла. В этом случае согласно массиву @outputs находится узел, на вход которого данные
	# должны быть отправлены, и данные отправляются в этот узел методом input.
	def _sendOutput(outputName, value)
		output = @outputs[outputName]
		if output.nil? then
			if $debug then puts "run(): Node #{_get_debug_name} tried to send value #{value} through output"\
				" '#{outputName}', but it does not connected to any inputs" end
			return
		end
		output.each do |pipe|
			node = pipe["node"]
			inputName = pipe["input"]
			sendValue = value.dup
			if $debug then puts "run(): Node #{_get_debug_name} sends value #{sendValue} through output '#{outputName}'"\
				" to input '#{inputName}' of node #{node._get_debug_name}" end
			node.input(inputName, sendValue)
		end
	end
	
	# Должен быть переопределен классами, наследующимися от Node. Должен возвращать true, если удалось выполнить какие-то
	# операции, иначе false (например если не на всех нужных входах есть данные).
	def run
		#should return true if performs some operations, othrwise should return false
		raise "No run method implemented"
	end
end

# Осуществляет печать строки, подаваемой на вход "input". Не имеет выходов.

class NodePrint < Node
	def initialize(name)
		super
		@inputs = {"input" => Queue.new}
		@inputTypes = {"input" => "STRING"}
		@outputs = {}
		@outputTypes = {}
	end
	def run
		if not _hasPending("input") then return false end
		puts("[Printer says] " + _takeFromInput("input"))
		true
	end
end

# Принимает два числовых входа. При первом такте (как только на входах min и max появились числа) подает на выход все числа
# от min до max (не включительно). При следующих тактах ничего не делает. Если бы была необходимость, можно было также
# реализовать ленивый бесконечный счетчик, для этого нужно проверять, является ли пустой очередь на следующем в цепочке узле

class NodeCounter < Node
	def initialize(name)
		super
		@inputs = {"min" => Queue.new, "max" => Queue.new}
		@inputTypes = {"min" => "INT", "max" => "INT"}
		@outputs = {"index" => Array.new}
		@outputTypes = {"index" => "INT"}
		@done = false
	end
	def run
		if @done then return false end
		if not _hasPending("min") or not _hasPending("max") then return false end
		(_takeFromInput("min") ... _takeFromInput("max")).each do |i|
			_sendOutput("index", i)
		end
		@done = true
		true
	end
end

# Вспомогательная функция для парсинга, например: "INT param1" -> ["INT", "param1"]. Строки для парсинга берутся из json-файла
# и подаются в виде параметра template_str в метод initialize параметризованного узла, откуда вызывается эта функция.

def parseTemplate(template_str) #example template_str: "INT param1,STRING param2"
	variables = template_str.split(',')
	result = Array.new
	variables.each do |variable|
		tokens = variable.strip.split(' ')
		result << tokens
	end
	result
end

# Форматирует строки. Вход input принимает строку и значения для форматирования. Имеет также выход error, куда пишет в
# случае ошибки.

class NodeFormat < Node
	def initialize(name, template_str)
		super
		@inputs = {"input" => Queue.new}
		@inputTypes = {"input" => "STRING"}
		@formatParamsOrder = []
		parseTemplate(template_str).each do |tokens|
			#for example, tokens[0] is "INT" and tokens[1] is "index"
			type = tokens[0]
			name = tokens[1]
			if not $types.has_key?(type) then raise "no such type " + type end
			@inputs[name] = Queue.new
			@inputTypes[name] = type
			@formatParamsOrder << name
		end
		@outputs = {"output" => Array.new, "error" => Array.new}
		@outputTypes = {"output" => "STRING", "error" => "STRING"}
	end
	def run
		@inputs.each do |inputName, inputQueue|
			if not _hasPending(inputName) then return false end
		end
		formatString = _takeFromInput("input")
		formatParams = []
		@formatParamsOrder.each do |formatParamName|
			formatParams << _takeFromInput(formatParamName)
		end
		begin
			result = format(formatString, *formatParams)
			_sendOutput("output", result)
		rescue ArgumentError => e
			_sendOutput("error", e.message)
		end
		true
	end
end

# Условное чтение файла. В данной реализации всегда возаращет изображение размером 800x600 с указанным именем.

class NodeReadJpg < Node
	def initialize(name)
		super
		@inputs = {"filename" => Queue.new}
		@inputTypes = {"filename" => "STRING"}
		@outputs = {"image" => Array.new, "error" => Array.new}
		@outputTypes = {"image" => "IMAGE", "error" => "STRING"}
	end
	def run
		if not _hasPending("filename") then return false end
		filename = _takeFromInput("filename")
		image = Image.new(800, 600)
		image.set_title(filename)
		_sendOutput("image", image)
		true
	end
end

# Превращает массив изображений в одно изображение. Имеет три входа.
# 	input - вход для изображений
# 	count - количество изображений, которые нужно взять из входа
# 	method - метод объединения изображений в одно, например медиана или усреднение
# Медиана изображений, например, может на практике использоваться для удаления помех.
# При каждом такте осуществляет попытку взять сразу count изображений из очереди input. Если их недостаточно,
# то взятые изображения сохраняет в массив @currentImages. Как только там накопится count изображений, выдает
# одно изображение на выходе. Для сравнения, в clojure ту же операцию можно реализовать с помощью
# partition и map с операцией медианы.

class NodeStack < Node
	def reset
		@currentCount = nil
		@currentMethod = nil
		@currentImages = nil
	end
	def initialize(name)
		super
		@inputs = {"input" => Queue.new, "count" => Queue.new, "method" => Queue.new}
		@inputTypes = {"input" => "IMAGE", "count" => "INT", "method" => "STRING"}
		@outputs = {"output" => Array.new, "error" => Array.new}
		@outputTypes = {"output" => "IMAGE", "error" => "STRING"}
		reset
	end
	def run
		if not @currentCount then
			if not _hasPending("count") or not _hasPending("method") then return false end
			@currentCount = _takeFromInput("count")
			@currentMethod = _takeFromInput("method")
			@currentImages = Array.new
		end
		if @currentCount < 1 then
			_sendOutput("error", "Invalid count: " + @currentCount)
			reset
			return true
		end
		if not _hasPending("input") then return false end
		while _hasPending("input") and @currentImages.length < @currentCount do
			@currentImages << _takeFromInput("input")
		end
		if @currentImages.length == @currentCount then
			w = @currentImages[0].instance_variable_get("@w")
			h = @currentImages[0].instance_variable_get("@h")
			titles = Array.new
			@currentImages.each do |image|
				if image.instance_variable_get("@w") != w or image.instance_variable_get("@h") != h then
					_sendOutput("error", "Image size mismatch")
					reset
					return true
				else
					titles << image.instance_variable_get("@title")
				end
			end
			if @currentMethod != "median" then
				_sendOutput("error", "Unsupported method: " + @currentMethod)
			else
				image = Image.new(w, h)
				image.operation("median [#{titles.join(", ")}]")
				_sendOutput("output", image)
			end
			reset
		end
		true
	end
end

# Условное сохранение изображения. Принимает на вход filename и image. В данной реализации ничего не сохраняет,
# а выводит на печать "saving image", информацию об изображении и список примененных к нему операций.

class NodeSaveJpg < Node
	def initialize(name)
		super
		@inputs = {"filename" => Queue.new, "image" => Queue.new}
		@inputTypes = {"filename" => "STRING", "image" => "IMAGE"}
		@outputs = {"error" => Array.new}
		@outputTypes = {"error" => "STRING"}
	end
	def run
		if not _hasPending("filename") or not _hasPending("image") then return false end
		filename = _takeFromInput("filename")
		image = _takeFromInput("image")
		w = image.instance_variable_get("@w")
		h = image.instance_variable_get("@h")
		title = image.instance_variable_get("@title")
		ops = image.instance_variable_get("@performedOperations")
		puts "[NodeSaveJpg] saving image: w #{w}, h #{h}, title #{title}, operations {#{ops.join(" AND ")}}"
		true
	end
end

# Данный узел был добавлен для демонстрации циклического конвейера (см. схему). Принимает изображение,
# возвращает изображение и логическую переменную max_enhanced. Она равна true, если изображение можно еще раз
# улучшить Enhancer'ом, то есть узел как бы говорит, что имеет смысл пропустить через него изображение еще раз.
# Следом за NodeEnhancer имеет смысл поставить узел NodeFilter (см. код ниже).

class NodeEnhancer < Node
	def initialize(name)
		super
		@inputs = {"input" => Queue.new}
		@inputTypes = {"input" => "IMAGE"}
		@outputs = {"output" => Array.new, "max_enhanced" => Array.new}
		@outputTypes = {"output" => "IMAGE", "max_enhanced" => "BOOL"}
	end
	def run
		if not _hasPending("input") then return false end
		image = _takeFromInput("input")
		ops = image.instance_variable_get("@performedOperations")
		if ops.length < 5 then image.operation("enhanced") end
		_sendOutput("output", image)
		_sendOutput("max_enhanced", ops.length >= 4)
		true
	end
end

# Принимает один параметризованный вход (т. е. тип входа должен быть указан в json-файле) и логическую
# переменную. В зависимости от значения переменной отправляет то, что подано на вход, по выходу output_true
# или по выходу output_false.

class NodeFilter < Node
	def initialize(name, template_str)
		super
		template = parseTemplate(template_str)
		if (template.length != 1 || template[0][1] != "input") then
			raise "Pass one template parameter named 'input'"
		end
		type = template[0][0]
		if not $types.has_key?(type) then raise "no such type " + type end
		@inputs = {"input" => Queue.new, "condition" => Queue.new}
		@inputTypes = {"input" => type, "condition" => "BOOL"}
		@outputs = {"output_true" => Array.new, "output_false" => Array.new}
		@outputTypes = {"output_true" => type, "output_false" => type}
	end
	def run
		if not _hasPending("input") or not _hasPending("condition") then return false end
		input = _takeFromInput("input")
		condition = _takeFromInput("condition")
		_sendOutput(condition ? "output_true" : "output_false", input)
		true
	end
end

# Функция run_pipeline:
# 1. Считывает конфиргурацию из json-файла
# 2. Собирает схему из узлов и констант согласно конфигурации
# 3. Вызывает метод run для всех узлов
# 4. Если хотя бы в одном узле метод run вернул true (то есть операция выполнена), то возвращается к шагу 3
# 5. сообщает о завершении работы конвейера

def run_pipeline(filename)
	file = File.open(filename)
	json = JSON.load(file)
	
	str_nodes = json["nodes"]
	str_constants = json["constants"]
	str_links = json["links"]
	puts "#{str_nodes.length} nodes, #{str_constants.length} constants, #{str_links.length} links"
	
	nodes = {}
	
	classnames = {
		"print" => NodePrint,
		"counter" => NodeCounter,
		"format" => NodeFormat,
		"read_jpg" => NodeReadJpg,
		"stack" => NodeStack,
		"save_jpg" => NodeSaveJpg,
		"enhancer" => NodeEnhancer,
		"filter" => NodeFilter
	}
	
	str_nodes.each do |name, str_class|
		tokens = str_class.split('<')
		classname = tokens[0]
		template = tokens.length == 1 ? nil : tokens[1][0..-2]
		nodeclass = classnames[classname]
		node = template ? nodeclass.new(name, template) : nodeclass.new(name)
		nodes[name] = node
	end
	
	str_constants.each do |input, const|
		input_tokens = input.split(".")
		nodename = input_tokens[0]
		nodeinput = input_tokens[1]
		node = nodes[nodename]
		node.assignConstant(nodeinput, const)
	end
	
	str_links.each do |output, inputs|
		output_tokens = output.split(".")
		output_nodename = output_tokens[0]
		output_node = nodes[output_nodename]
		output_name = output_tokens[1]
		if inputs.class != Array then inputs = [inputs] end
		inputs.each do |input|
			input_tokens = input.split(".")
			input_nodename = input_tokens[0]
			input_node = nodes[input_nodename]
			input_name = input_tokens[1]
			output_node.connectOutput(output_name, input_node, input_name)
		end
	end
	
	while true do
		wereChanges = false
		nodes.each do |nodename, node|
			wereChanges |= node.run()
		end
		if not wereChanges then break end
	end
	
	puts "Pipeline finished work"
end

run_pipeline("C:/Users/Oleg/Desktop/script.json")

#nodeprint = NodePrint.new("printer")
#nodeformat = NodeFormat.new("format", "INT param1, INT param2")
#nodeformat.connectOutput("output", nodeprint, "input")
#nodeformat.assignConstant("param1", 1)
#nodeformat.input("param2", 2)
#nodeformat.input("input", "qwerty")
#nodeformat.run()
#nodeprint.run()
#puts nodeformat._hasPending("param1")