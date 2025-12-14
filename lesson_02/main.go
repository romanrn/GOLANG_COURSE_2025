package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	var n int
	fmt.Print("Input number: ")
	_, err := fmt.Scan(&n)

	fmt.Println(" Provided number n = ", n)
	fmt.Println(" for example n=10 , expected result = 55")
	_, _ = fmt.Println("FibonacciIterative: ", FibonacciIterative(n))
	_, _ = fmt.Println("FibonacciRecursive: ", FibonacciRecursive(n))

	fmt.Println("################")
	result := IsPrime(n)

	fmt.Printf("Is Provided number n = %d Prime ? \n", n)

	fmt.Println("", result)
	fmt.Println("################")

	palindrome := IsBinaryPalindrome(n)
	fmt.Println("IsBinaryPalindrome: ", palindrome)
	fmt.Println("################")

	fmt.Print("Input string like ([{}]) or [{]} or ( fg) [111 { crtrt }*** ]: ")

	reader := bufio.NewReader(os.Stdin)
	input, err2 := reader.ReadString('\n')

	input = strings.TrimSpace(input)

	if err != nil || err2 != nil {
		if err != nil {
			fmt.Println("Error reading input:", err)
		} else {
			fmt.Println("Error reading input:", err2)
		}

		return
	}
	isParentheses := ValidParentheses(input)
	//fmt.Printf("Is Provided number n = %d Prime ? \n", n)
	fmt.Printf("Is %s ValidParentheses? \n", input)
	fmt.Printf("Answ: %t\n", isParentheses)
	fmt.Println("################")
	fmt.Println(`Increment("101") ->`, Increment("0101")) // => 6
	fmt.Println(Increment("111"))                         // (4 + 2 + 1) = 7 => 8
	fmt.Println(Increment("1000"))                        // 8. => 9
	fmt.Println(Increment("0110111"))                     // 55 => 56

	/*
		// Невеликі демонстраційні виклики (для наочного запуску `go run .`)
		fmt.Println("FibonacciIterative(10):", FibonacciIterative(10)) // очікуємо 55
		fmt.Println("FibonacciRecursive(10):", FibonacciRecursive(10)) // очікуємо 55

		result, _ := IsPrime(2)
		fmt.Println("IsPrime(2):", result) // true
		result, _ = IsPrime(15)
		fmt.Println("IsPrime(15):", result) // false
		result, _ = IsPrime(29)
		fmt.Println("IsPrime(29):", result) // true

		fmt.Println("IsBinaryPalindrome(7):", IsBinaryPalindrome(7)) // true (111)
		fmt.Println("IsBinaryPalindrome(6):", IsBinaryPalindrome(6)) // false (110)

		fmt.Println(`ValidParentheses("[]{}()"):`, ValidParentheses("[]{}()")) // true
		fmt.Println(`ValidParentheses("[{]}"):`, ValidParentheses("[{]}"))     // false

		fmt.Println(`Increment("101") ->`, Increment("0101")) // => 6
		fmt.Println(Increment("111"))     // (4 + 2 + 1) = 7 => 8
		fmt.Println(Increment("1000"))    // 8. => 9
		fmt.Println(Increment("0110111")) // 55 => 56
	*/
}

func FibonacciIterative(n int) int {
	// TODO: імплементуйте ітераційно.
	// Підказка: тримайте два останні значення й оновлюйте їх у циклі.
	// Вхід вважаємо: n >= 0.
	// При отриманні негативного n повертаємо його без змін
	// F(n) = F(n-1) + F(n-2)
	// 0 1 1 2 3 5 8 13 21 ...
	if n < 0 {
		fmt.Println("negative input")
		return n
	}
	if n == 0 || n == 1 {
		return n
	}
	a, b := 0, 1

	for i := 2; i <= n; i++ {
		// Оновлюємо значення:
		// a стає попереднім b
		// b стає сумою (a + b)
		// У Go це можна зробити в один рядок (паралельне присвоєння)
		/*
			c = b
			b = a + b
			a = c
		*/

		a, b = b, a+b
	}
	return b
}

func FibonacciRecursive(n int) int {
	// TODO: імплементуйте рекурсивно.
	// База: n==0 -> 0; n==1 -> 1.
	// Рекурсія: F(n-1)+F(n-2)
	// При отриманні негативного n повертаємо його без змін
	// 0 1 1 2 3 5 8 13 21 ...

	if n < 0 || n == 0 || n == 1 {
		return n
	}

	return FibonacciRecursive(n-1) + FibonacciRecursive(n-2)

}

func IsPrime(n int) bool {
	// TODO: імплементуйте перевірку на просте число.
	// Підказка: n<=1 -> false; 2 -> true; парні >2 -> false;
	// Далі перевіряйте дільники до sqrt(n).
	if n <= 1 {
		return false
	}
	if n == 2 {
		return true
	}
	if n%2 == 0 {
		return false
	}
	// step +2
	// i*i equals math.Sqrt
	for i := 3; i*i <= n; i += 2 {
		if n%i == 0 {
			return false
		}
	}

	return true
}

func IsBinaryPalindrome(n int) bool {
	// TODO: імплементуйте перевірку числа на те що це паліндром.
	// Підказка: перетворіть n у строку (strconv ефективніший за fmt)
	// потім перевірте паліндромність.

	if n == 0 || n == 1 {
		return true
	}
	var bits []int
	for n > 0 {
		bits = append([]int{n % 2}, bits...)
		n /= 2
	}
	for i, j := 0, len(bits)-1; i < j; i, j = i+1, j-1 {
		if bits[i] != bits[j] {
			return false
		}
	}
	fmt.Println("Binary representation:", bits)
	return true
}

func ValidParentheses(s string) bool {
	// TODO: імплементуйте перевірку дужок.
	// Правила:
	// 1. Допустимі дужки (, [, {, ), ], }
	// 2. У кожної відкритої дужки є відповідна закриваюча дужка того ж типу
	// 3. Закриваючі дужки стоять у правильному порядку
	// "[{}]" - правильно
	// "[{]}" - не правильно
	// 4. Кожна закриваюча дужка має відповідну відкриваючу дужку
	// Підказка: використовуйте стек (можна зробити через масив рун []rune)

	// ( = 40
	// ) = 41
	// ] = 93
	// [ = 91
	// { = 123
	// } = 125

	if len(s) == 0 {
		fmt.Println("Empty input string")
		return false
	}

	stack := []rune{}

	for _, r := range s {
		switch r {
		case '(', '[', '{':
			stack = append(stack, r) // push
			fmt.Println("push:", string(r), "stack:", stack)
		case ')', ']', '}':
			if len(stack) == 0 {
				fmt.Println("closing without opening:", string(r))
				return false
			}
			top := stack[len(stack)-1]
			stack = stack[:len(stack)-1] // pop
			fmt.Println("pop:", string(top), "matches with", string(r), "stack now:", stack)

			if (r == ')' && top != '(') ||
				(r == ']' && top != '[') ||
				(r == '}' && top != '{') {
				fmt.Println("mismatch:", string(top), "vs", string(r))
				return false
			}
		default:
			fmt.Println("Skip character:", string(r))
		}
	}

	if len(stack) != 0 {
		fmt.Println("unclosed brackets left in stack:", stack)
		return false
	}

	return true
}

func Increment(num string) int {
	// TODO: Імплементуйте функцію яка на вхід отримує строку яка складається лише з символів 0 та 1
	// Тобто строка містить певне число у бінарному вигляді
	// Потрібно повернути число на один більше
	// Додайте валідацію вхідної строки, якщо вона містить недопустимі символи, повертайте 0

	// 0101 (4 + 0 + 1)     5+1 =  6
	// 0111 (4 + 2 + 1)     7+1 =  8
	// 1000  (8 + 0 + 0 +0) 8+1 =  9

	result := 0
	multiplier := 1 //  2^0 = 1

	for i := len(num) - 1; i >= 0; i-- {
		if num[i] != '1' && num[i] != '0' {
			fmt.Println("Incorrect inout", num[i])
			return 0
		}
		if num[i] == '1' {
			result += multiplier
		}

		// shift (1 -> 2 -> 4 -> 8 -> 16)
		multiplier *= 2
	}

	return result + 1
}
