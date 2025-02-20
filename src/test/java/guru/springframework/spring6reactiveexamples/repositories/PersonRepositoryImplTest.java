package guru.springframework.spring6reactiveexamples.repositories;

import guru.springframework.spring6reactiveexamples.domain.Person;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

class PersonRepositoryImplTest {

    PersonRepository repo = new PersonRepositoryImpl();

    @Test
    void testMonoByIdBlock() { // Come NON fare un test con Mono
        Mono<Person> personMono = repo.getById(1);

        Person person = personMono.block();

        assert person != null;

        System.out.println(person);
    }

    @Test
    void testGetByIdSubscriber() {
        Mono<Person> personMono = repo.getById(1);

        personMono.subscribe(System.out::println);
    }

    @Test
    void testMapOperation() {
        Mono<Person> personMono = repo.getById(1);

        personMono.map(Person::getFirstName).subscribe(System.out::println);
    }

    @Test
    void testFluxBlockFirst() { // Come NON fare un test con Flux
        Flux<Person> personFlux = repo.findAll();

        Person person = personFlux.blockFirst();

        System.out.println(person);
    }

    @Test
    void testFluxSubscriber() {
        Flux<Person> personFlux = repo.findAll();

        personFlux.subscribe(System.out::println);

        personFlux.subscribe(person -> System.out.println(person));
    }

    @Test
    void testFluxMap() {
        Flux<Person> personFlux = repo.findAll();

        personFlux.map(Person::getFirstName).subscribe(System.out::println);

        personFlux.map(person -> person.getFirstName()).subscribe(firstName -> System.out.println(firstName));
    }

    @Test
    void testFluxToList() {
        Flux<Person> personFlux = repo.findAll();

        Mono<List<Person>> listMono = personFlux.collectList();

        listMono.subscribe(System.out::println);

        listMono.subscribe(list -> list.forEach(System.out::println));

        listMono.subscribe(list -> list.forEach(person -> System.out.println(person.getFirstName())));
    }

}